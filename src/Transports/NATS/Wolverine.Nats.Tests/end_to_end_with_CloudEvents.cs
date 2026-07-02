using System.Text;
using System.Text.Json;
using JasperFx.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Net;
using Shouldly;
using IntegrationTests;
using Wolverine.Tracking;
using Xunit;
using Xunit.Abstractions;

namespace Wolverine.Nats.Tests;

// Ascendium interop fork: end-to-end coverage for the InteropWithCloudEvents() surface that the
// fork adds to the NATS transport. Two angles:
//   * raw_cloudevents_from_an_external_producer_is_consumed — the headline use case: a non-Wolverine
//     producer drops a CloudEvents JSON document on a subject, and a Wolverine listener configured
//     with InteropWithCloudEvents() deserializes + dispatches it to the handler.
//   * wolverine_to_wolverine_round_trip — both ends speak CloudEvents over NATS.
public record ColorMessage(string Color);

public class ColorMessageHandler
{
    public static void Handle(ColorMessage message)
    {
        ColorMessageSink.Record(message);
    }
}

// Sequential test execution is enforced assembly-wide (NoParallelization.cs), so a static sink
// with an explicit Reset() per test is safe and lets us await externally-published messages that
// Wolverine's TrackActivity (which only tracks Wolverine-originated sends) cannot observe.
public static class ColorMessageSink
{
    private static TaskCompletionSource<ColorMessage> _next = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public static void Reset() => _next = new TaskCompletionSource<ColorMessage>(TaskCreationOptions.RunContinuationsAsynchronously);

    public static void Record(ColorMessage message) => _next.TrySetResult(message);

    public static async Task<ColorMessage> WaitForNextAsync(TimeSpan timeout)
    {
        var completed = await Task.WhenAny(_next.Task, Task.Delay(timeout));
        if (completed != _next.Task)
        {
            throw new TimeoutException($"No ColorMessage received within {timeout}.");
        }

        return await _next.Task;
    }
}

[Collection("NATS Integration Tests")]
[Trait("Category", "Integration")]
public class end_to_end_with_CloudEvents : IAsyncLifetime
{
    private const string MessageTypeAlias = "ascendium.nats.tests.color";
    private readonly ITestOutputHelper _output;
    private static int _counter;
    private IHost? _receiver;
    private IHost? _sender;
    private string _subject = "";

    public end_to_end_with_CloudEvents(ITestOutputHelper output)
    {
        _output = output;
    }

    public Task InitializeAsync()
    {
        ColorMessageSink.Reset();
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        if (_sender != null)
        {
            await _sender.StopAsync();
            _sender.Dispose();
        }

        if (_receiver != null)
        {
            await _receiver.StopAsync();
            _receiver.Dispose();
        }
    }

    private static string? NatsUrl => Environment.GetEnvironmentVariable("NATS_URL");

    [Fact]
    public async Task raw_cloudevents_from_an_external_producer_is_consumed()
    {
        var natsUrl = NatsUrl;
        if (string.IsNullOrEmpty(natsUrl)) return; // matches the existing integration-test skip pattern

        _subject = $"cloudevents.inbound.{++_counter}";

        _receiver = await Host.CreateDefaultBuilder()
            .ConfigureLogging(l => l.AddXunitLogging(_output))
            .UseWolverine(opts =>
            {
                opts.UseNats(natsUrl).AutoProvision();
                opts.RegisterMessageType(typeof(ColorMessage), MessageTypeAlias);
                opts.ListenToNatsSubject(_subject).Named("receiver").InteropWithCloudEvents().ProcessInline();
                opts.Discovery.IncludeAssembly(GetType().Assembly);
            })
            .StartAsync();

        // A non-Wolverine producer puts a raw CloudEvents 1.0 JSON document straight onto the subject.
        var cloudEvent = new
        {
            specversion = "1.0",
            type = MessageTypeAlias,
            source = "some-external-system",
            id = Guid.NewGuid().ToString(),
            time = DateTimeOffset.UtcNow.ToString("O"),
            datacontenttype = "application/json",
            data = new { color = "yellow" }
        };
        var payload = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(cloudEvent));

        await using (var raw = new NatsConnection(new NatsOpts { Url = natsUrl }))
        {
            await raw.ConnectAsync();
            await raw.PublishAsync(_subject, payload);
        }

        var received = await ColorMessageSink.WaitForNextAsync(30.Seconds());
        received.Color.ShouldBe("yellow");
    }

    [Fact]
    public async Task wolverine_to_wolverine_round_trip_over_cloudevents()
    {
        var natsUrl = NatsUrl;
        if (string.IsNullOrEmpty(natsUrl)) return;

        _subject = $"cloudevents.roundtrip.{++_counter}";

        _receiver = await Host.CreateDefaultBuilder()
            .ConfigureLogging(l => l.AddXunitLogging(_output))
            .UseWolverine(opts =>
            {
                opts.UseNats(natsUrl).AutoProvision();
                opts.RegisterMessageType(typeof(ColorMessage), MessageTypeAlias);
                opts.ListenToNatsSubject(_subject).Named("receiver").InteropWithCloudEvents().ProcessInline();
                opts.Discovery.IncludeAssembly(GetType().Assembly);
            })
            .StartAsync();

        _sender = await Host.CreateDefaultBuilder()
            .ConfigureLogging(l => l.AddXunitLogging(_output))
            .UseWolverine(opts =>
            {
                opts.UseNats(natsUrl).AutoProvision();
                opts.Policies.DisableConventionalLocalRouting();
                opts.RegisterMessageType(typeof(ColorMessage), MessageTypeAlias);
                opts.PublishAllMessages().ToNatsSubject(_subject).SendInline().InteropWithCloudEvents();
            })
            .StartAsync();

        await _sender.TrackActivity()
            .Timeout(60.Seconds())
            .AlsoTrack(_receiver)
            .WaitForMessageToBeReceivedAt<ColorMessage>(_receiver)
            .PublishMessageAndWaitAsync(new ColorMessage("green"));

        var received = await ColorMessageSink.WaitForNextAsync(5.Seconds());
        received.Color.ShouldBe("green");
    }
}
