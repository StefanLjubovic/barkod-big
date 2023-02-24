package flights.producer;

import flights.generator.Generator;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import radar.FlightUpdateEvent;

public class FlightProducer extends Producer {
    private static final Logger logger = LogManager.getLogger(FlightProducer.class);
    private final Generator generator;

    public FlightProducer(Properties configuration) {
        super(configuration);
        this.topic = configuration.getProperty("kafka.topic.flight.update.events");
        this.inputResourceName = configuration.getProperty("airports.input.resource");
        this.generator = new Generator(inputResourceName);
    }

    @Override
    public void run() {
        while (true) {
            FlightUpdateEvent flight = this.generator.generateFlight();

            this.producer.send(new ProducerRecord<>(topic, flight.getId().toString(), flight));
            logger.info(String.format("Published event to %s", topic));
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
