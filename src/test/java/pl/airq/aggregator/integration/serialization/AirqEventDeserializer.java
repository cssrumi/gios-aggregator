package pl.airq.aggregator.integration.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import pl.airq.common.process.EventParser;
import pl.airq.common.process.Payload;
import pl.airq.common.process.event.AirqEvent;

public class AirqEventDeserializer<T extends Payload> implements Deserializer<AirqEvent<T>> {

    private final StringDeserializer stringDeserializer = new StringDeserializer();
    private final EventParser parser;

    public AirqEventDeserializer(EventParser parser) {
        this.parser = parser;
    }

    @Override
    @SuppressWarnings("unchecked")
    public AirqEvent<T> deserialize(String topic, byte[] data) {
        return parser.deserializeDomainEvent(stringDeserializer.deserialize(topic, data));
    }
}
