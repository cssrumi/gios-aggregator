package pl.airq.aggregator.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import pl.airq.common.process.EventParser;
import pl.airq.common.process.Payload;
import pl.airq.common.process.event.AirqEvent;

public class AirqEventSerde<T extends Payload> implements Serde<AirqEvent<T>> {

    private final EventParser parser;
    private final Serde<String> stringSerde;

    public AirqEventSerde(EventParser parser) {
        this.parser = parser;
        this.stringSerde = Serdes.String();
    }

    @Override
    public Serializer<AirqEvent<T>> serializer() {
        return (topic, data) -> stringSerde.serializer().serialize(topic, parser.parse(data));
    }

    @Override
    public Deserializer<AirqEvent<T>> deserializer() {
        return (topic, data) -> parser.deserializeDomainEvent(stringSerde.deserializer().deserialize(topic, data));
    }
}
