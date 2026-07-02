using System.Reflection;
using Shouldly;
using Wolverine.Configuration;
using Wolverine.Nats.Configuration;
using Wolverine.Nats.Internal;
using Wolverine.Transports;
using Xunit;

namespace Wolverine.Nats.Tests;

// Ascendium interop fork: these are pure type/surface assertions (no broker) that lock in the
// four-point augmentation that adds UseInterop / InteropWithCloudEvents to the NATS transport,
// mirroring how Kafka exposes the same surface. If a future re-fork drops the generic
// Endpoint<,> base or the InteroperableListenerConfiguration base, these fail fast.
public class InteropConfigurationSurfaceTests
{
    [Fact]
    public void nats_envelope_mapper_implements_the_interop_marker_interface()
    {
        typeof(INatsEnvelopeMapper).IsAssignableFrom(typeof(NatsEnvelopeMapper)).ShouldBeTrue();
    }

    [Fact]
    public void interop_marker_is_an_envelope_mapper_over_the_core_nats_message_shape()
    {
        typeof(IEnvelopeMapper<global::NATS.Client.Core.NatsMsg<byte[]>, global::NATS.Client.Core.NatsHeaders>)
            .IsAssignableFrom(typeof(INatsEnvelopeMapper)).ShouldBeTrue();
    }

    [Fact]
    public void nats_endpoint_derives_from_the_generic_interoperable_endpoint_base()
    {
        // NatsEndpoint : Endpoint<INatsEnvelopeMapper, NatsEnvelopeMapper>
        var expected = typeof(Endpoint<,>).MakeGenericType(typeof(INatsEnvelopeMapper), typeof(NatsEnvelopeMapper));
        typeof(NatsEndpoint).BaseType.ShouldBe(expected);
    }

    [Fact]
    public void listener_configuration_derives_from_the_interoperable_listener_base()
    {
        var expected = typeof(InteroperableListenerConfiguration<,,,>)
            .MakeGenericType(typeof(NatsListenerConfiguration), typeof(NatsEndpoint),
                typeof(INatsEnvelopeMapper), typeof(NatsEnvelopeMapper));
        typeof(NatsListenerConfiguration).BaseType.ShouldBe(expected);
    }

    [Fact]
    public void subscriber_configuration_derives_from_the_interoperable_subscriber_base()
    {
        var expected = typeof(InteroperableSubscriberConfiguration<,,,>)
            .MakeGenericType(typeof(NatsSubscriberConfiguration), typeof(NatsEndpoint),
                typeof(INatsEnvelopeMapper), typeof(NatsEnvelopeMapper));
        typeof(NatsSubscriberConfiguration).BaseType.ShouldBe(expected);
    }

    [Theory]
    [InlineData(typeof(NatsListenerConfiguration))]
    [InlineData(typeof(NatsSubscriberConfiguration))]
    public void both_configurations_expose_the_interop_methods(Type configType)
    {
        configType.GetMethods().Where(m => m.Name == "UseInterop").ShouldNotBeEmpty();
        configType.GetMethod("InteropWithCloudEvents").ShouldNotBeNull();
    }
}
