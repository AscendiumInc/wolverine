using IntegrationTests;
using JasperFx.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using Shouldly;
using Wolverine.Attributes;
using Wolverine.Configuration;
using Wolverine.ErrorHandling;
using Wolverine.Nats.Internal;
using Xunit;
using Xunit.Abstractions;

namespace Wolverine.Nats.Tests;

/// <summary>
/// Coverage for the NATS dead-letter path, which had no tests at all — which is how the defect below
/// shipped.
///
/// <para>
/// <c>MessageContext.MoveToDeadLetterQueueAsync</c> routes to a listener's <em>native</em> dead-letter
/// queue whenever the listener reports <c>NativeDeadLetterQueueEnabled</c>, and then returns — it never
/// falls through to <c>Storage.Inbox.MoveToDeadLetterStorageAsync</c>. Its caller
/// (<c>MoveToErrorQueue.ExecuteAsync</c>) then calls <c>CompleteAsync()</c>, which acks. So a
/// <c>MoveToErrorsAsync</c> that returns without retaining the message loses it outright.
/// </para>
///
/// <para>
/// <c>NatsListener.MoveToErrorsAsync</c> did exactly that: it gated on JetStream's <c>NumDelivered</c>
/// against <c>EffectiveMaxDeliveryAttempts</c>. Those two counters measure different things —
/// <c>Envelope.Attempts</c> counts in-process attempts within a single delivery, while
/// <c>NumDelivered</c> counts broker deliveries and only advances on NAK or AckWait expiry. Any
/// in-process retry policy (including Wolverine's own <c>RetryWithCooldown</c>) therefore left
/// <c>NumDelivered</c> at 1, tripped the guard, and the message was acked and dropped: nothing on the
/// dead-letter subject, nothing in durable storage, nothing left in the stream.
/// </para>
/// </summary>
public class NatsDeadLetterStorageModeTests
{
    private static NatsEndpoint EndpointWith(bool useJetStream, bool dlqEnabled, string? dlqSubject)
    {
        var transport = new NatsTransport();
        var endpoint = transport.EndpointForSubject("dlqmode.subject");
        endpoint.UseJetStream = useJetStream;
        endpoint.DeadLetterQueueEnabled = dlqEnabled;
        endpoint.DeadLetterSubject = dlqSubject;
        return endpoint;
    }

    [Fact]
    public void jetstream_with_a_dead_letter_subject_is_native()
    {
        EndpointWith(useJetStream: true, dlqEnabled: true, dlqSubject: "dlqmode.errors")
            .DeadLetterStorage.ShouldBe(DeadLetterStorageMode.Native);
    }

    [Fact]
    public void jetstream_without_a_dead_letter_subject_is_durable()
    {
        // There is nowhere native to publish to, so the listener falls back to durable storage.
        EndpointWith(useJetStream: true, dlqEnabled: true, dlqSubject: null)
            .DeadLetterStorage.ShouldBe(DeadLetterStorageMode.Durable);
    }

    [Fact]
    public void dead_letter_queueing_disabled_is_durable()
    {
        EndpointWith(useJetStream: true, dlqEnabled: false, dlqSubject: "dlqmode.errors")
            .DeadLetterStorage.ShouldBe(DeadLetterStorageMode.Durable);
    }

    [Fact]
    public void core_nats_is_always_durable()
    {
        // Core NATS has no message to terminate; CoreNatsSubscriber reports no native DLQ support.
        EndpointWith(useJetStream: false, dlqEnabled: true, dlqSubject: "dlqmode.errors")
            .DeadLetterStorage.ShouldBe(DeadLetterStorageMode.Durable);
    }

    [Fact]
    public void the_default_endpoint_is_durable()
    {
        new NatsTransport().EndpointForSubject("dlqmode.default")
            .DeadLetterStorage.ShouldBe(DeadLetterStorageMode.Durable);
    }
}

[Collection("NATS Integration")]
[Trait("Category", "Integration")]
public class NatsDeadLetterQueueTests
{
    private readonly ITestOutputHelper _output;

    public NatsDeadLetterQueueTests(ITestOutputHelper output) => _output = output;

    /// <summary>
    /// The regression. An in-process retry policy never advances the broker's <c>NumDelivered</c>, so
    /// before the fix this message reached neither the dead-letter subject nor durable storage — it was
    /// acked and dropped while Wolverine logged "was moved to the error queue".
    /// </summary>
    [Fact]
    public async Task a_poison_message_reaches_the_dead_letter_subject_under_an_in_process_retry_policy()
    {
        var natsUrl = NatsTestHelpers.ResolveUrl();
        if (!await NatsTestHelpers.IsNatsAvailable(natsUrl)) return;

        var id = Guid.NewGuid().ToString("N");
        var stream = $"DLQ_{id}";
        var root = $"dlq.{id}";
        var subject = $"{root}.work";
        var dlqSubject = $"{root}.dead";

        PoisonHandler.Reset();

        // Subscribe to the dead-letter subject BEFORE the poison message can be dead-lettered, so the
        // assertion cannot pass on a message that was never published.
        await using var connection = new NatsConnection(new NatsOpts { Url = natsUrl });
        await connection.ConnectAsync();

        using var subscriberCancellation = new CancellationTokenSource(60.Seconds());
        var deadLettered = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var watching = Task.Run(async () =>
        {
            await foreach (var msg in connection.SubscribeAsync<string>(dlqSubject)
                               .WithCancellation(subscriberCancellation.Token))
            {
                deadLettered.TrySetResult(msg.Data ?? string.Empty);
                return;
            }
        }, subscriberCancellation.Token);

        using var host = await Host.CreateDefaultBuilder()
            .ConfigureLogging(l => l.AddXunitLogging(_output))
            .UseWolverine(opts =>
            {
                opts.UseNats(natsUrl)
                    .AutoProvision()
                    // The stream must be DECLARED for the transport to provision it: NatsTransport
                    // only provisions Configuration.Streams, which DefineStream populates. Naming a
                    // stream on the listener alone fails at startup with "stream not found".
                    .DefineStream(stream, s => s.WithSubjects($"{root}.>"));

                opts.Policies.DisableConventionalLocalRouting();

                // In-process retries only — this is the shape that broke. NumDelivered stays at 1.
                opts.Policies.OnAnyException()
                    .RetryWithCooldown(50.Milliseconds(), 100.Milliseconds(), 250.Milliseconds());

                opts.ListenToNatsSubject(subject)
                    .UseJetStream(stream, $"dlq-consumer-{id}")
                    .ConfigureDeadLetterQueue(3, dlqSubject);

                opts.PublishMessage<PoisonMessage>().ToNatsSubject(subject).UseJetStream(stream)
                    .SendInline();
            })
            .StartAsync();

        await host.MessageBus().SendAsync(new PoisonMessage(id));

        // The handler must actually have run and failed, otherwise the dead-letter assertion is vacuous.
        await PoisonHandler.WaitForFirstAsync();

        var payload = await deadLettered.Task.WaitAsync(45.Seconds());
        payload.ShouldContain(id);

        await subscriberCancellation.CancelAsync();
    }

    /// <summary>
    /// The threshold is the broker's, not the policy's. Even with a <c>ConfigureDeadLetterQueue</c>
    /// threshold well above the number of in-process attempts, Wolverine's decision to dead-letter is
    /// final — the broker's redelivery budget does not get a veto over it.
    /// </summary>
    [Fact]
    public async Task a_high_max_delivery_threshold_does_not_suppress_the_dead_letter()
    {
        var natsUrl = NatsTestHelpers.ResolveUrl();
        if (!await NatsTestHelpers.IsNatsAvailable(natsUrl)) return;

        var id = Guid.NewGuid().ToString("N");
        var stream = $"DLQHIGH_{id}";
        var root = $"dlqhigh.{id}";
        var subject = $"{root}.work";
        var dlqSubject = $"{root}.dead";

        PoisonHandler.Reset();

        await using var connection = new NatsConnection(new NatsOpts { Url = natsUrl });
        await connection.ConnectAsync();

        using var subscriberCancellation = new CancellationTokenSource(60.Seconds());
        var deadLettered = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var watching = Task.Run(async () =>
        {
            await foreach (var msg in connection.SubscribeAsync<string>(dlqSubject)
                               .WithCancellation(subscriberCancellation.Token))
            {
                deadLettered.TrySetResult(msg.Data ?? string.Empty);
                return;
            }
        }, subscriberCancellation.Token);

        using var host = await Host.CreateDefaultBuilder()
            .ConfigureLogging(l => l.AddXunitLogging(_output))
            .UseWolverine(opts =>
            {
                opts.UseNats(natsUrl)
                    .AutoProvision()
                    .DefineStream(stream, s => s.WithSubjects($"{root}.>"));

                opts.Policies.DisableConventionalLocalRouting();
                opts.Policies.OnAnyException().RetryWithCooldown(50.Milliseconds());

                opts.ListenToNatsSubject(subject)
                    .UseJetStream(stream, $"dlqhigh-consumer-{id}")
                    // 100 broker deliveries — a NumDelivered guard would never let this through.
                    .ConfigureDeadLetterQueue(100, dlqSubject);

                opts.PublishMessage<PoisonMessage>().ToNatsSubject(subject).UseJetStream(stream)
                    .SendInline();
            })
            .StartAsync();

        await host.MessageBus().SendAsync(new PoisonMessage(id));
        await PoisonHandler.WaitForFirstAsync();

        var payload = await deadLettered.Task.WaitAsync(45.Seconds());
        payload.ShouldContain(id);

        await subscriberCancellation.CancelAsync();
    }
}

public record PoisonMessage(string Id);

[WolverineHandler]
public static class PoisonHandler
{
    private static TaskCompletionSource _first = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public static void Reset()
    {
        _first = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public static void Handle(PoisonMessage message)
    {
        _first.TrySetResult();
        throw new InvalidOperationException($"poison {message.Id}");
    }

    public static Task WaitForFirstAsync()
    {
        return _first.Task.WaitAsync(30.Seconds());
    }
}
