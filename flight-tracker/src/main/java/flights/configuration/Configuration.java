package flights.configuration;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

public class Configuration {

    public static Properties streamsProperties(Properties configuration) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, configuration.getProperty("kafka.application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getProperty("kafka.bootstrap.servers"));
        props.put(
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put("schema.registry.url", configuration.getProperty("kafka.schema.registry.url"));
        props.put("value.subject.name.strategy", "io.confluent.kafka.serializers.subject.RecordNameStrategy");


        return props;
    }

    public static Properties loadProperties(String resourceFileName) throws IOException {
        Properties configuration = new Properties();
        InputStream inputStream = Configuration.class.getClassLoader().getResourceAsStream(resourceFileName);
        configuration.load(inputStream);
        inputStream.close();

        return configuration;
    }
}
