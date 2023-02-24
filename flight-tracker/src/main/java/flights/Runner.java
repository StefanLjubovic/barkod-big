package flights;

import flights.configuration.Configuration;
import flights.topology.TopologyBuilder;
import java.io.IOException;
import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.log4j.LogManager;

public class Runner {
    private static final org.apache.log4j.Logger logger = LogManager.getLogger(Runner.class);

    public static void main(String[] args) throws IOException {
        Properties properties = Configuration.loadProperties("application.properties");
        TopologyBuilder topologyBuilder = new TopologyBuilder(properties);

        start(topologyBuilder.build(), Configuration.streamsProperties(properties));
    }

    private static void start(Topology topology, Properties properties) {
        logger.info(topology.describe().toString());

        KafkaStreams app = new KafkaStreams(topology, properties);
        logger.info("Starting Kafka Streams application.");
        app.start();
        // Gracefully close Stream app on SIGINT.
        Runtime.getRuntime().addShutdownHook(new Thread(app::close));
    }
}
