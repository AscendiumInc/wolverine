using NATS.Client.Core;
using Wolverine.Transports;

namespace Wolverine.Nats.Internal;

/// <summary>
/// Marker interface for the Core NATS envelope mapper, mirroring Kafka's
/// <c>IKafkaEnvelopeMapper</c>. It is the <c>TMapper</c> type parameter of the
/// generic <see cref="Wolverine.Configuration.Endpoint{TMapper,TConcreteMapper}"/>
/// that <see cref="NatsEndpoint"/> derives from, which is what enables the
/// <c>UseInterop</c> / <c>InteropWithCloudEvents</c> configuration surface on the
/// NATS listener/subscriber configurations.
///
/// Ascendium interop fork addition (not present upstream at V6.5.1).
/// </summary>
public interface INatsEnvelopeMapper : IEnvelopeMapper<NatsMsg<byte[]>, NatsHeaders>;
