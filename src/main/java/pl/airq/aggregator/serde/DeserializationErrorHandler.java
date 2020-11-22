package pl.airq.aggregator.serde;

import io.quarkus.runtime.annotations.RegisterForReflection;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RegisterForReflection
public class DeserializationErrorHandler implements DeserializationExceptionHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeserializationErrorHandler.class);

    @Override
    public DeserializationHandlerResponse handle(ProcessorContext context, ConsumerRecord<byte[], byte[]> record, Exception exception) {
        final String key;
        final String value;
        if (record == null) {
            key = null;
            value = null;
        } else {
            key = record.key() != null ? new String(record.key()) : null;
            value = record.value() != null ? new String(record.value()) : null;
        }
        LOGGER.error("Error during deserialization of {} - {}", key, value, exception);
        return DeserializationHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
