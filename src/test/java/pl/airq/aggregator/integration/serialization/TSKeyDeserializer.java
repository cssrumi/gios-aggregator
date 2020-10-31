package pl.airq.aggregator.integration.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import pl.airq.common.store.key.TSKey;

public class TSKeyDeserializer implements Deserializer<TSKey> {

    private final StringDeserializer stringDeserializer = new StringDeserializer();

    @Override
    public TSKey deserialize(String topic, byte[] data) {
        return TSKey.from(stringDeserializer.deserialize(topic, data));
    }
}
