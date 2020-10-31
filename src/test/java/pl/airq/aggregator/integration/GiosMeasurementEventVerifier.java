package pl.airq.aggregator.integration;

import com.google.common.base.Preconditions;
import java.time.OffsetDateTime;
import pl.airq.common.process.ctx.gios.aggragation.GiosMeasurementEventPayload;
import pl.airq.common.process.event.AirqEvent;
import pl.airq.common.store.key.TSKey;

import static org.assertj.core.api.Assertions.assertThat;

public class GiosMeasurementEventVerifier {

    private final AirqEvent<GiosMeasurementEventPayload> event;
    private final Class<? extends AirqEvent> eventClass;
    private final TSKey key;
    private final String station;
    private final Float pm10;
    private final Float pm25;

    private GiosMeasurementEventVerifier(AirqEvent<GiosMeasurementEventPayload> event,
                                        Class<? extends AirqEvent> eventClass,
                                        TSKey key,
                                        String station,
                                        Float pm10,
                                        Float pm25) {
        Preconditions.checkNotNull(event);
        Preconditions.checkNotNull(eventClass);
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(station);
        this.event = event;
        this.eventClass = eventClass;
        this.key = key;
        this.station = station;
        this.pm10 = pm10;
        this.pm25 = pm25;
    }

    public void verify() {
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

    public static Builder builder() {
        return new Builder();
    }

    private static class Builder {
        private AirqEvent<GiosMeasurementEventPayload> event;
        private Class<? extends AirqEvent> eventClass;
        private TSKey key;
        private String station;
        private Float pm10;
        private Float pm25;

        private Builder() {
        }

        public Builder withEvent(AirqEvent<GiosMeasurementEventPayload> event) {
            this.event = event;
            return this;
        }

        public Builder withEventClass(Class<? extends AirqEvent> eventClass) {
            this.eventClass = eventClass;
            return this;
        }

        public Builder withKey(TSKey key) {
            this.key = key;
            return this;
        }

        public Builder withStation(String station) {
            this.station = station;
            return this;
        }

        public Builder withPm10(Float pm10) {
            this.pm10 = pm10;
            return this;
        }

        public Builder withPm25(Float pm25) {
            this.pm25 = pm25;
            return this;
        }

        public GiosMeasurementEventVerifier build() {
            return new GiosMeasurementEventVerifier(event, eventClass, key, station, pm10, pm25);
        }

    }

}
