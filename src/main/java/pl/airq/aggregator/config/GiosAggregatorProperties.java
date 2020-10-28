package pl.airq.aggregator.config;

import io.quarkus.arc.config.ConfigProperties;
import java.time.temporal.ChronoUnit;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@ConfigProperties(prefix = "gios")
public class GiosAggregatorProperties {

    @NotNull
    private Topology topology;
    @NotEmpty
    private String installationTopic;
    @NotEmpty
    private String measurementTopic;
    @NotEmpty
    private String measurementStore;

    public Topology getTopology() {
        return topology;
    }

    public void setTopology(Topology topology) {
        this.topology = topology;
    }

    public String getInstallationTopic() {
        return installationTopic;
    }

    public void setInstallationTopic(String installationTopic) {
        this.installationTopic = installationTopic;
    }

    public String getMeasurementStore() {
        return measurementStore;
    }

    public void setMeasurementStore(String measurementStore) {
        this.measurementStore = measurementStore;
    }

    public String getMeasurementTopic() {
        return measurementTopic;
    }

    public void setMeasurementTopic(String measurementTopic) {
        this.measurementTopic = measurementTopic;
    }

    public static class Topology {

        @NotNull
        private DurationConfig windowSize;
        @NotNull
        private DurationConfig retentionPeriod;

        public DurationConfig getWindowSize() {
            return windowSize;
        }

        public void setWindowSize(DurationConfig windowSize) {
            this.windowSize = windowSize;
        }

        public DurationConfig getRetentionPeriod() {
            return retentionPeriod;
        }

        public void setRetentionPeriod(DurationConfig retentionPeriod) {
            this.retentionPeriod = retentionPeriod;
        }
    }

    public static class DurationConfig {

        @NotNull
        private ChronoUnit timeUnit;
        @Min(1)
        private long amount;

        public ChronoUnit getTimeUnit() {
            return timeUnit;
        }

        public void setTimeUnit(ChronoUnit timeUnit) {
            this.timeUnit = timeUnit;
        }

        public long getAmount() {
            return amount;
        }

        public void setAmount(long amount) {
            this.amount = amount;
        }
    }
}
