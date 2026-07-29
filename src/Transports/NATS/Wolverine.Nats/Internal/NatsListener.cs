using JasperFx.Blocks;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using Wolverine.Runtime;
using Wolverine.Transports;
using Wolverine.Transports.Sending;

namespace Wolverine.Nats.Internal;

public class NatsListener : IListener, ISupportDeadLetterQueue, IReportConnectionState
{
    private readonly NatsEndpoint _endpoint;
    private readonly IWolverineRuntime _runtime;
    private readonly IReceiver _receiver;
    private readonly ILogger<NatsEndpoint> _logger;
    private readonly CancellationTokenSource _cancellation;
    private readonly RetryBlock<NatsEnvelope> _complete;
    private readonly RetryBlock<NatsEnvelope> _defer;
    private readonly INatsSubscriber _subscriber;
    private readonly ISender _deadLetterSender;

    public IHandlerPipeline? Pipeline { get; private set; }

    // GH-3231: surface the NATS connection state (via the subscriber that owns the connection) so external monitors
    // can detect a listener whose connection has dropped while it still reports Accepting.
    public TransportConnectionState ConnectionState => _subscriber.ConnectionState;

    internal NatsListener(
        NatsEndpoint endpoint,
        INatsSubscriber subscriber,
        IWolverineRuntime runtime,
        IReceiver receiver,
        ILogger<NatsEndpoint> logger,
        ISender deadLetterSender,
        CancellationToken parentCancellation
    )
    {
        _endpoint = endpoint;
        _subscriber = subscriber;
        _runtime = runtime;
        _receiver = receiver;
        _logger = logger;
        _deadLetterSender = deadLetterSender;
        _cancellation = CancellationTokenSource.CreateLinkedTokenSource(parentCancellation);
        Address = endpoint.Uri;

        _complete = new RetryBlock<NatsEnvelope>(
            async (envelope, _) =>
            {
                if (envelope.JetStreamMsg != null)
                {
                    await envelope.JetStreamMsg.AckAsync(
                        cancellationToken: _cancellation.Token
                    );
                }
            },
            logger,
            _cancellation.Token
        );

        _defer = new RetryBlock<NatsEnvelope>(
            async (envelope, _) =>
            {
                if (envelope.JetStreamMsg != null)
                {
                    // JetStream supports native NAK which will redeliver the message
                    await envelope.JetStreamMsg.NakAsync(
                        cancellationToken: _cancellation.Token
                    );
                }
                else
                {
                    // Core NATS doesn't have native requeue - republish the message to the subject
                    await _subscriber.RepublishAsync(envelope, _cancellation.Token);
                }
            },
            logger,
            _cancellation.Token
        );
    }

    public Uri Address { get; }

    public bool NativeDeadLetterQueueEnabled => _subscriber.SupportsNativeDeadLetterQueue;

    /// <remarks>
    /// Wolverine calls this <em>only</em> once its error policy has already decided to dead-letter, and
    /// <c>MessageContext.MoveToDeadLetterQueueAsync</c> <c>return</c>s immediately afterwards — it never
    /// falls through to <c>Storage.Inbox.MoveToDeadLetterStorageAsync</c>. The continuation's very next act
    /// is <c>CompleteAsync()</c>, which acks. <strong>So every path out of this method must retain the
    /// message somewhere.</strong> A bare <c>return</c> here is not "decline to handle it" — it is silent
    /// message loss.
    /// </remarks>
    public async Task MoveToErrorsAsync(Envelope envelope, Exception exception)
    {
        // Nothing to terminate on the broker (Core NATS, or a listener whose native DLQ is off and was
        // selected anyway). Persist rather than return — see the remarks above.
        if (envelope is not NatsEnvelope natsEnvelope || !NativeDeadLetterQueueEnabled ||
            natsEnvelope.JetStreamMsg == null)
        {
            await MoveToDurableStorageAsync(
                envelope,
                exception,
                "the envelope carries no JetStream message to terminate");
            return;
        }

        var metadata = natsEnvelope.JetStreamMsg.Metadata;

        // NOTE: there is deliberately no `NumDelivered < EffectiveMaxDeliveryAttempts` guard here.
        // The two counters measure different things: Envelope.Attempts counts IN-PROCESS attempts within
        // a single delivery, while JetStream's NumDelivered counts BROKER deliveries and only advances on
        // NAK or AckWait expiry. An in-process retry policy (Wolverine's default RetryWithCooldown) never
        // advances NumDelivered, so gating on it made this method return without publishing or terminating
        // on every such configuration — and the caller then acked the message away. Wolverine owns the
        // decision to dead-letter; the broker's redelivery budget does not get a veto over it.
        var attempts = (int)(metadata?.NumDelivered ?? 1);

        // Retain the poison message by forwarding a copy to the dead-letter subject BEFORE terminating,
        // so a terminate failure can't lose it. With no dead-letter subject configured there is nowhere
        // native to put it, so fall back to durable storage instead of dropping it.
        if (!string.IsNullOrEmpty(_endpoint.DeadLetterSubject))
        {
            envelope.Attempts = attempts;
            DeadLetterQueueConstants.StampFailureMetadata(envelope, exception);
            envelope.Headers["x-dlq-original-subject"] = _endpoint.Subject;

            await _deadLetterSender.SendAsync(envelope);
        }
        else
        {
            _logger.LogWarning(
                exception,
                "Message {MessageId} dead-lettered after {Attempts} broker delivery attempts on subject {Subject}, but no dead-letter subject is configured; falling back to durable dead-letter storage. Use DeadLetterTo(...) / ConfigureDeadLetterQueue(...) to retain poison messages on a NATS subject.",
                envelope.Id,
                attempts,
                _endpoint.Subject
            );

            await MoveToDurableStorageAsync(envelope, exception, "no dead-letter subject is configured");
        }

        // Terminate delivery on the JetStream consumer with a reason so the server stops redelivering and
        // records why the message was dead-lettered.
        await natsEnvelope.JetStreamMsg.AckTerminateAsync(
            $"wolverine: exceeded {attempts} delivery attempts ({exception.GetType().Name})",
            cancellationToken: _cancellation.Token
        );

        _logger.LogError(
            exception,
            "Message {MessageId} terminated after {Attempts} delivery attempts. Subject: {Subject}, DeadLetter: {DeadLetter}",
            envelope.Id,
            attempts,
            _endpoint.Subject,
            _endpoint.DeadLetterSubject ?? "(none)"
        );
    }

    /// <summary>
    /// Last-resort retention: hand the envelope to Wolverine's durable dead-letter storage when the
    /// native NATS path cannot take it. <c>MessageContext</c> will not do this for us — once it has
    /// routed to a native dead-letter queue it returns without consulting storage — so the listener
    /// performs the fall-through itself rather than letting the caller's <c>CompleteAsync()</c> ack an
    /// unretained message. No-ops harmlessly when the service has no message store
    /// (<c>NullMessageStore</c>), which is why the reason is logged either way.
    /// </summary>
    private async Task MoveToDurableStorageAsync(Envelope envelope, Exception exception, string reason)
    {
        try
        {
            await _runtime.Storage.Inbox.MoveToDeadLetterStorageAsync(envelope, exception);

            _logger.LogInformation(
                "Message {MessageId} on subject {Subject} was dead-lettered to durable storage because {Reason}.",
                envelope.Id,
                _endpoint.Subject,
                reason
            );
        }
        catch (Exception storageFailure)
        {
            // Both destinations are now unavailable. Say so at Error with the original failure attached —
            // this is the one case where the message really is lost, and it must never be silent.
            _logger.LogError(
                new AggregateException(exception, storageFailure),
                "Message {MessageId} on subject {Subject} could not be dead-lettered natively ({Reason}) and durable dead-letter storage also failed. The message is lost.",
                envelope.Id,
                _endpoint.Subject,
                reason
            );
        }
    }

    public async ValueTask CompleteAsync(Envelope envelope)
    {
        if (envelope is NatsEnvelope natsEnvelope)
        {
            await _complete.PostAsync(natsEnvelope);
        }
    }

    public async ValueTask DeferAsync(Envelope envelope)
    {
        if (envelope is NatsEnvelope natsEnvelope)
        {
            await _defer.PostAsync(natsEnvelope);
        }
    }

    public async Task StartAsync()
    {
        await _subscriber.StartAsync(this, _receiver, _cancellation.Token);
    }

    public async ValueTask StopAsync()
    {
        await _cancellation.CancelAsync();
        await _subscriber.DisposeAsync();
    }

    public void Dispose()
    {
        _cancellation.Cancel();
        _cancellation.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        if (!_cancellation.IsCancellationRequested)
        {
            await _cancellation.CancelAsync();
            await _subscriber.DisposeAsync();
        }

        _complete.Dispose();
        _defer.Dispose();
        _cancellation.Dispose();
    }

    internal static NatsListener Create(
        NatsEndpoint endpoint,
        NatsConnection connection,
        INatsJSContext? jetStreamContext,
        IWolverineRuntime runtime,
        IReceiver receiver,
        ILogger<NatsEndpoint> logger,
        ISender? deadLetterSender,
        CancellationToken cancellation,
        bool useJetStream,
        string? subscriptionPattern = null,
        ITenantSubjectMapper? tenantMapper = null
    )
    {
        INatsSubscriber subscriber;
        if (useJetStream)
        {
            var jsMapper = new JetStreamEnvelopeMapper(endpoint, tenantMapper);
            if (endpoint.MessageType != null)
            {
                jsMapper.ReceivesMessage(endpoint.MessageType);
            }
            subscriber = new JetStreamSubscriber(endpoint, connection, jetStreamContext!, logger, jsMapper, subscriptionPattern);
        }
        else
        {
            // Ascendium interop fork: use the endpoint's (possibly UseInterop-customized) Core
            // NATS mapper rather than building a fresh default. NatsEndpoint.buildMapper folds in
            // the tenant subject mapper, and Endpoint<,>.BuildMapper applies ReceivesMessage +
            // any UseInterop customization. Upstream built `new NatsEnvelopeMapper(endpoint, tenantMapper)`.
            var mapper = endpoint.EnvelopeMapper ??= endpoint.BuildMapper(runtime);
            subscriber = new CoreNatsSubscriber(endpoint, connection, logger, mapper, subscriptionPattern);
        }

        return new NatsListener(
            endpoint,
            subscriber,
            runtime,
            receiver,
            logger,
            deadLetterSender ?? new NullSender(),
            cancellation
        );
    }
}
