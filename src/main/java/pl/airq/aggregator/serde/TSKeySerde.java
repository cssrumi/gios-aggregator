package pl.airq.aggregator.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import pl.airq.common.store.key.TSKey;

public class TSKeySerde implements Serde<TSKey> {

    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Override
    public Serializer<TSKey> serializer() {
        return (topic, key) -> STRING_SERDE.serializer().serialize(topic, key.value());
    }

    @Override
    public Deserializer<TSKey> deserializer() {
        return (topic, data) -> TSKey.from(STRING_SERDE.deserializer().deserialize(topic, data));
    }
}
