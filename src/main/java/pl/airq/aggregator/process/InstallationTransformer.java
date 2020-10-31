package pl.airq.aggregator.process;

import com.google.common.collect.Iterators;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.NoSuchElementException;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import pl.airq.common.domain.gios.GiosMeasurement;
import pl.airq.common.domain.gios.Installation;
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

public class InstallationTransformer implements ValueTransformerWithKey<TSKey,
        AirqEvent<GiosInstallationEventPayload>, AirqEvent<GiosMeasurementEventPayload>> {

    private final String giosMeasurementStore;
    private final Duration windowSize;
    private WindowStore<TSKey, GiosMeasurement> stateStore;

    public InstallationTransformer(String giosMeasurementStore, Duration windowSize) {
        this.giosMeasurementStore = giosMeasurementStore;
        this.windowSize = windowSize;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.stateStore = (WindowStore<TSKey, GiosMeasurement>) context.getStateStore(giosMeasurementStore);
    }

    @Override
    public AirqEvent<GiosMeasurementEventPayload> transform(TSKey key, AirqEvent<GiosInstallationEventPayload> value) {
        final Installation installation = value.payload.installation;
        if (value instanceof GiosInstallationCreatedEvent) {
            return createHandler(key, installation);
        }
        if (value instanceof GiosInstallationUpdatedEvent) {
            return updateHandler(key, installation);
        }
        if (value instanceof GiosInstallationDeletedEvent) {
            return deleteHandler(key, installation);
        }
        return null;
    }

    @Override
    public void close() {
    }

    private AirqEvent<GiosMeasurementEventPayload> createHandler(TSKey key, Installation installation) {
        final GiosMeasurement storeValue = findLast(key);
        if (storeValue == null) {
            final GiosMeasurement newValue = GiosMeasurement.from(installation);
            stateStore.put(key, newValue);
            return new GiosMeasurementCreatedEvent(OffsetDateTime.now(), new GiosMeasurementEventPayload(newValue));
        }

        final GiosMeasurement newValue = storeValue.merge(installation);
        stateStore.put(key, newValue);
        return new GiosMeasurementUpdatedEvent(OffsetDateTime.now(), new GiosMeasurementEventPayload(newValue));
    }

    private AirqEvent<GiosMeasurementEventPayload> updateHandler(TSKey key, Installation installation) {
        return createHandler(key, installation);
    }

    private AirqEvent<GiosMeasurementEventPayload> deleteHandler(TSKey key, Installation installation) {
        final GiosMeasurement last = findLast(key);
        if (last == null) {
            return null;
        }

        final GiosMeasurement updated = last.remove(installation);
        if (updated.pm10 == null && updated.pm25 == null) {
            stateStore.put(key, null);
            return new GiosMeasurementDeletedEvent(OffsetDateTime.now(), new GiosMeasurementEventPayload(last));
        }

        stateStore.put(key, updated);
        return new GiosMeasurementUpdatedEvent(OffsetDateTime.now(), new GiosMeasurementEventPayload(updated));
    }

    private GiosMeasurement findLast(TSKey key) {
        Instant now = Instant.now();
        Instant from = now.minus(windowSize);
        final WindowStoreIterator<GiosMeasurement> fetch = stateStore.fetch(key, from, now);
        final KeyValue<Long, GiosMeasurement> last;
        try {
             last = Iterators.getLast(fetch);
        } catch (NoSuchElementException ignore) {
            return null;
        }

        return last != null ? last.value : null;
    }
}
