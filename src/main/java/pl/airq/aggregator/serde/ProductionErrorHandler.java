package pl.airq.aggregator.serde;

import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProductionErrorHandler implements ProductionExceptionHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProductionErrorHandler.class);

    @Override
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record, Exception exception) {
        final String key;
        final String value;
        if (record == null) {
            key = null;
            value = null;
        } else {
            key = record.key() != null ? new String(record.key()) : null;
            value = record.value() != null ? new String(record.value()) : null;
        }
        LOGGER.error("Error during producing of {} - {}", key, value, exception);
        return ProductionExceptionHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
