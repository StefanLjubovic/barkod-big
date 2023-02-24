package flights.producer;

import java.util.Properties;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public abstract class Producer implements Runnable {
    protected org.apache.kafka.clients.producer.Producer<String, SpecificRecord> producer;
    protected String topic;
    protected String inputResourceName;

    public Producer(Properties configuration) {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getProperty("kafka.bootstrap.servers"));
        props.put(ProducerConfig.CLIENT_ID_CONFIG, configuration.getProperty("kafka.application.id"));
        props.put(ProducerConfig.ACKS_CONFIG, configuration.getProperty("kafka.producer.acks"));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, configuration.getProperty("kafka.topic.compression.type"));

        props.put("schema.registry.url", configuration.getProperty("kafka.schema.registry.url"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.subject.name.strategy", "io.confluent.kafka.serializers.subject.RecordNameStrategy");

        this.producer = new KafkaProducer<>(props);
    }
}
