package pl.airq.aggregator.integration.serialization;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import pl.airq.common.process.EventParser;
import pl.airq.common.process.Payload;
import pl.airq.common.process.event.AirqEvent;

public class AirqEventSerializer<T extends Payload> implements Serializer<AirqEvent<T>> {

    private final StringSerializer stringSerializer = new StringSerializer();
    private final EventParser parser;

    public AirqEventSerializer(EventParser parser) {
        this.parser = parser;
    }

    @Override
    public byte[] serialize(String topic, AirqEvent<T> data) {
        return stringSerializer.serialize(topic, parser.parse(data));
    }
}
