package pl.airq.aggregator.process;

import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import java.time.Duration;
import java.util.Objects;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import pl.airq.aggregator.config.GiosAggregatorProperties;
import pl.airq.aggregator.serde.AirqEventSerde;
import pl.airq.aggregator.serde.TSKeySerde;
import pl.airq.common.domain.gios.GiosMeasurement;
import pl.airq.common.process.EventParser;
import pl.airq.common.process.ctx.gios.aggragation.GiosMeasurementEventPayload;
import pl.airq.common.process.ctx.gios.installation.GiosInstallationEventPayload;
import pl.airq.common.process.event.AirqEvent;
import pl.airq.common.store.key.TSKey;

@ApplicationScoped
public class TopologyProducer {

    private final Duration retentionPeriod;
    private final Duration windowSize;
    private final String installationTopic;
    private final String measurementTopic;
    private final String giosMeasurementStore;

    @Inject
    public TopologyProducer(GiosAggregatorProperties properties) {
        this.retentionPeriod = Duration.of(
                properties.getTopology().getRetentionPeriod().getAmount(),
                properties.getTopology().getRetentionPeriod().getTimeUnit());
        this.windowSize = Duration.of(
                properties.getTopology().getWindowSize().getAmount(),
                properties.getTopology().getWindowSize().getTimeUnit());
        this.installationTopic = properties.getInstallationTopic();
        this.measurementTopic = properties.getMeasurementTopic();
        this.giosMeasurementStore = properties.getMeasurementStore();
    }

    @Produces
    public Topology buildTopology(EventParser parser) {
        StreamsBuilder builder = new StreamsBuilder();
        Serde<TSKey> keySerde = new TSKeySerde();
        Serde<GiosMeasurement> storeValueSerde = new ObjectMapperSerde<>(GiosMeasurement.class);
        Serde<AirqEvent<GiosInstallationEventPayload>> installationEventSerde = new AirqEventSerde<>(parser);
        Serde<AirqEvent<GiosMeasurementEventPayload>> measurementEventSerde = new AirqEventSerde<>(parser);

        WindowBytesStoreSupplier storeSupplier = Stores.inMemoryWindowStore(
                giosMeasurementStore, retentionPeriod, windowSize, false);

        builder.addStateStore(Stores.windowStoreBuilder(storeSupplier, keySerde, storeValueSerde));

        builder.table(installationTopic, Consumed.with(keySerde, installationEventSerde))
               .transformValues(
                       () -> new InstallationTransformer(giosMeasurementStore, retentionPeriod),
                       giosMeasurementStore
               )
               .toStream()
               .filter((key, value) -> Objects.nonNull(value))
               .to(measurementTopic, Produced.with(keySerde, measurementEventSerde));

        return builder.build();
    }
}
