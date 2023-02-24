package flights;

import flights.configuration.Configuration;
import flights.producer.AirportProducer;
import flights.producer.FlightProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.Properties;

public class Runner {
    private static final Logger logger = LogManager.getLogger(Runner.class);

    public static void main(String[] args) {
        try {
            start();
        } catch (IOException error) {
            logger.error(error);
            logger.info("Exiting application.");
            System.exit(1);
        }
    }

    public static void start() throws IOException {
        logger.info("Starting producers.");

        Properties properties = Configuration.loadProperties("application.properties");
        new Thread(new AirportProducer(properties)).start();
        new Thread(new FlightProducer(properties)).start();
    }
}
