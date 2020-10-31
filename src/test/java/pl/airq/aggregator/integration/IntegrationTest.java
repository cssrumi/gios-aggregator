package pl.airq.aggregator.integration;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.awaitility.core.ConditionTimeoutException;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import pl.airq.aggregator.config.GiosAggregatorProperties;
import pl.airq.aggregator.integration.serialization.AirqEventDeserializer;
import pl.airq.aggregator.integration.serialization.AirqEventSerializer;
import pl.airq.aggregator.integration.serialization.TSKeyDeserializer;
import pl.airq.aggregator.integration.serialization.TSKeySerializer;
import pl.airq.common.domain.gios.Installation;
import pl.airq.common.process.EventParser;
import pl.airq.common.process.ctx.gios.aggragation.GiosMeasurementCreatedEvent;
import pl.airq.common.process.ctx.gios.aggragation.GiosMeasurementDeletedEvent;
import pl.airq.common.process.ctx.gios.aggragation.GiosMeasurementEventPayload;
import pl.airq.common.process.ctx.gios.aggragation.GiosMeasurementUpdatedEvent;
import pl.airq.common.process.ctx.gios.installation.GiosInstallationCreatedEvent;
import pl.airq.common.process.ctx.gios.installation.GiosInstallationDeletedEvent;
import pl.airq.common.process.ctx.gios.installation.GiosInstallationEventPayload;
import pl.airq.common.process.ctx.gios.installation.GiosInstallationUpdatedEvent;
import pl.airq.common.process.event.AirqEvent;
import pl.airq.common.store.key.TSKey;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@QuarkusTestResource(KafkaResource.class)
@QuarkusTest
class IntegrationTest {

    private final Map<TSKey, AirqEvent<GiosMeasurementEventPayload>> events = new ConcurrentHashMap<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final AtomicBoolean shouldConsume = new AtomicBoolean(true);

    @Inject
    GiosAggregatorProperties properties;
    @Inject
    KafkaProducer<TSKey, AirqEvent<GiosInstallationEventPayload>> producer;
    @Inject
    KafkaConsumer<TSKey, AirqEvent<GiosMeasurementEventPayload>> consumer;

    @BeforeAll
    void startConsuming() {
        consumer.subscribe(List.of(properties.getMeasurementTopic()));
        executor.submit(() -> {
            while (shouldConsume.get()) {
                consumer.poll(Duration.ofMillis(100))
                        .records(properties.getMeasurementTopic())
                        .forEach(record -> events.put(record.key(), record.value()));
            }
        });
    }

    @AfterAll
    void stopConsuming() {
        shouldConsume.set(false);
        executor.shutdown();
    }

    @BeforeEach
    void clearEvents() {
        events.clear();
    }

    @Test
    void whenGiosInstallationCreatedArrived_expectGiosMeasurementCreatedProduced() {
        String station = StationFactory.next();
        Installation installation = InstallationFactory.create(station, Field.PM10);
        GiosInstallationEventPayload payload = new GiosInstallationEventPayload(installation);
        GiosInstallationCreatedEvent iEvent = new GiosInstallationCreatedEvent(OffsetDateTime.now(), payload);
        final TSKey key = sendEvent(iEvent);

        final AirqEvent<GiosMeasurementEventPayload> event = awaitForLatestEvent(key, Duration.ofSeconds(0));

        verifyGiosMeasurementEvent(event, GiosMeasurementCreatedEvent.class, key, station, installation.value, null);
    }

    @Test
    void whenGiosInstallationCreatedAndUpdatedArrived_expectGiosMeasurementUpdatedProduced() {
        String station = StationFactory.next();
        Installation installation1 = InstallationFactory.create(station, Field.PM10);
        Installation installation2 = InstallationFactory.create(station, Field.PM10);
        GiosInstallationEventPayload payload1 = new GiosInstallationEventPayload(installation1);
        GiosInstallationEventPayload payload2 = new GiosInstallationEventPayload(installation2);
        GiosInstallationCreatedEvent event1 = new GiosInstallationCreatedEvent(OffsetDateTime.now(), payload1);
        GiosInstallationUpdatedEvent event2 = new GiosInstallationUpdatedEvent(OffsetDateTime.now(), payload2);
        final TSKey key1 = sendEvent(event1);
        final TSKey key2 = sendEvent(event2);

        final AirqEvent<GiosMeasurementEventPayload> event = awaitForLatestEvent(key2, Duration.ofSeconds(2));

        assertThat(key1).isEqualTo(key2);
        verifyGiosMeasurementEvent(event, GiosMeasurementUpdatedEvent.class, key2, station, installation2.value, null);
    }

    @Test
    void whenGiosInstallationCreatedForBothPm10AndPm25Arrived_expectGiosMeasurementUpdatedProduced() {
        String station = StationFactory.next();
        Installation installation1 = InstallationFactory.create(station, Field.PM10);
        Installation installation2 = InstallationFactory.create(station, Field.PM25);
        GiosInstallationEventPayload payload1 = new GiosInstallationEventPayload(installation1);
        GiosInstallationEventPayload payload2 = new GiosInstallationEventPayload(installation2);
        GiosInstallationCreatedEvent event1 = new GiosInstallationCreatedEvent(OffsetDateTime.now(), payload1);
        GiosInstallationUpdatedEvent event2 = new GiosInstallationUpdatedEvent(OffsetDateTime.now(), payload2);
        final TSKey key1 = sendEvent(event1);
        final TSKey key2 = sendEvent(event2);

        final AirqEvent<GiosMeasurementEventPayload> event = awaitForLatestEvent(key2, Duration.ofSeconds(2));

        assertThat(key1).isEqualTo(key2);
        verifyGiosMeasurementEvent(event, GiosMeasurementUpdatedEvent.class, key2, station, installation1.value, installation2.value);
    }

    @Test
    void whenGiosInstallationCreatedAndDeletedArrived_expectGiosMeasurementDeletedProduced() {
        String station = StationFactory.next();
        Installation installation1 = InstallationFactory.create(station, Field.PM10);
        Installation installation2 = InstallationFactory.create(station, Field.PM10, (Float)null);
        GiosInstallationEventPayload payload1 = new GiosInstallationEventPayload(installation1);
        GiosInstallationEventPayload payload2 = new GiosInstallationEventPayload(installation2);
        GiosInstallationCreatedEvent event1 = new GiosInstallationCreatedEvent(OffsetDateTime.now(), payload1);
        GiosInstallationDeletedEvent event2 = new GiosInstallationDeletedEvent(OffsetDateTime.now(), payload2);
        final TSKey key1 = sendEvent(event1);
        final TSKey key2 = sendEvent(event2);

        final AirqEvent<GiosMeasurementEventPayload> event = awaitForLatestEvent(key2, Duration.ofSeconds(2));

        assertThat(key1).isEqualTo(key2);
        verifyGiosMeasurementEvent(event, GiosMeasurementDeletedEvent.class, key2, station, installation1.value, null);
    }

    @Test
    void whenGiosInstallationCreatedForBothPm10AndPm25AndDeletedPm10Arrived_expectGiosMeasurementUpdatedProduced() {
        String station = StationFactory.next();
        Installation installation1 = InstallationFactory.create(station, Field.PM10);
        Installation installation2 = InstallationFactory.create(station, Field.PM25);
        Installation installation3 = InstallationFactory.create(station, Field.PM10, (Float)null);
        GiosInstallationEventPayload payload1 = new GiosInstallationEventPayload(installation1);
        GiosInstallationEventPayload payload2 = new GiosInstallationEventPayload(installation2);
        GiosInstallationEventPayload payload3 = new GiosInstallationEventPayload(installation3);
        GiosInstallationCreatedEvent event1 = new GiosInstallationCreatedEvent(OffsetDateTime.now(), payload1);
        GiosInstallationCreatedEvent event2 = new GiosInstallationCreatedEvent(OffsetDateTime.now(), payload2);
        GiosInstallationDeletedEvent event3 = new GiosInstallationDeletedEvent(OffsetDateTime.now(), payload3);
        final TSKey key1 = sendEvent(event1);
        final TSKey key2 = sendEvent(event2);
        final TSKey key3 = sendEvent(event3);

        final AirqEvent<GiosMeasurementEventPayload> event = awaitForLatestEvent(key2, Duration.ofSeconds(2));

        assertThat(key1).isEqualTo(key2).isEqualTo(key3);
        verifyGiosMeasurementEvent(event, GiosMeasurementUpdatedEvent.class, key3, station, null, installation2.value);
    }

    @Test
    void whenGiosInstallationCreatedWithKeyOlderThanNowMinusWindowArrived_expectNothingProduced() {
        String station = StationFactory.next();
        final Duration windowDuration = Duration.of(properties.getTopology().getWindowSize().getAmount(), properties.getTopology().getWindowSize().getTimeUnit());
        final OffsetDateTime timestamp = OffsetDateTime.now().minus(windowDuration).minusMinutes(1);
        Installation installation1 = InstallationFactory.create(station, Field.PM10, timestamp);
        GiosInstallationEventPayload payload1 = new GiosInstallationEventPayload(installation1);
        GiosInstallationCreatedEvent event1 = new GiosInstallationCreatedEvent(OffsetDateTime.now(), payload1);
        final TSKey key1 = sendEvent(event1);

        assertThatThrownBy(() -> awaitForLatestEvent(key1, Duration.ofSeconds(0)))
                .isInstanceOf(ConditionTimeoutException.class);
    }

    private AirqEvent<GiosMeasurementEventPayload> awaitForLatestEvent(TSKey key, Duration maxAwait) {
        await().atMost(Duration.ofSeconds(5)).until(() -> events.containsKey(key));
        try {
            Thread.sleep(maxAwait.toMillis());
        } catch (InterruptedException ignore) {
        }
        return events.get(key);
    }

    private void verifyGiosMeasurementEvent(AirqEvent<GiosMeasurementEventPayload> event,
                                            Class<? extends AirqEvent> eventClass, TSKey key,
                                            String station, Float pm10, Float pm25) {
        assertThat(event).isNotNull();
        assertThat(event.eventType()).isEqualTo(eventClass.getSimpleName());
        assertThat(event.timestamp).isBeforeOrEqualTo(OffsetDateTime.now());
        assertThat(event.payload.measurement).isNotNull();
        assertThat(event.payload.measurement.pm10).isEqualTo(pm10);
        assertThat(event.payload.measurement.pm25).isEqualTo(pm25);
        assertThat(event.payload.measurement.station).isNotNull();
        assertThat(event.payload.measurement.station.id).isNotNull();
        assertThat(event.payload.measurement.station.id.value()).isEqualTo(station);
        assertThat(event.payload.measurement.timestamp.toEpochSecond())
                .isEqualTo(key.timestamp().toEpochSecond());
    }

    private TSKey sendEvent(AirqEvent<GiosInstallationEventPayload> event) {
        TSKey key = TSKey.from(event.payload.installation);
        final Future<RecordMetadata> future = producer
                .send(new ProducerRecord<>(properties.getInstallationTopic(), key, event));
        try {
            future.get();
            return key;
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Dependent
    static class KafkaClientConfiguration {

        @ConfigProperty(name = "quarkus.kafka-streams.bootstrap-servers")
        String bootstrapServers;
        @Inject
        GiosAggregatorProperties giosAggregatorProperties;

        @Produces
        KafkaProducer<TSKey, AirqEvent<GiosInstallationEventPayload>> stringKafkaProducer(EventParser parser) {
            Properties properties = new Properties();
            properties.put("bootstrap.servers", bootstrapServers);

            return new KafkaProducer<>(properties, new TSKeySerializer(), new AirqEventSerializer<>(parser));
        }

        @Produces
        KafkaConsumer<TSKey, AirqEvent<GiosMeasurementEventPayload>> kafkaConsumer(EventParser parser) {
            Properties properties = new Properties();
            properties.put("bootstrap.servers", bootstrapServers);
            properties.put("enable.auto.commit", "true");
            properties.put("group.id", "airq-data-enrichment-int-test");
            properties.put("auto.offset.reset", "earliest");

            KafkaConsumer<TSKey, AirqEvent<GiosMeasurementEventPayload>> consumer = new KafkaConsumer<>(
                    properties, new TSKeyDeserializer(), new AirqEventDeserializer<>(parser));
            consumer.subscribe(Collections.singleton(giosAggregatorProperties.getMeasurementTopic()));
            return consumer;
        }
    }
}
