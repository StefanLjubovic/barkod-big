package flights;

import flights.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

abstract class TopologyTest {
    private static final Logger logger = LogManager.getLogger(TopologyTest.class);

    protected Topology topology;
    protected Properties props = Configuration.loadProperties("application.properties");

    protected TopologyTestDriver driver;

    protected TopologyTest() throws IOException {}

    void createInputTopics(TopologyTestDriver driver) {}

    void createOutputTopics(TopologyTestDriver driver) {}

    void deleteStateDir() {
        try {
            FileUtils.deleteDirectory(new File(props.getProperty("kafka.state.dir")));
        } catch (IOException e) {
            logger.info("Couldn't remove state directory.");
            logger.error(e);
        }
    }
}
