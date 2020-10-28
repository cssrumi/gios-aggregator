package pl.airq.aggregator.process;

import java.time.OffsetDateTime;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
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
    private KeyValueStore<TSKey, GiosMeasurement> stateStore;

    public InstallationTransformer(String giosMeasurementStore) {
        this.giosMeasurementStore = giosMeasurementStore;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.stateStore = (KeyValueStore<TSKey, GiosMeasurement>) context.getStateStore(giosMeasurementStore);
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
        final GiosMeasurement storeValue = stateStore.get(key);
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
        final GiosMeasurement deleted = stateStore.delete(key);
        return new GiosMeasurementDeletedEvent(OffsetDateTime.now(), new GiosMeasurementEventPayload(deleted));
    }

}
