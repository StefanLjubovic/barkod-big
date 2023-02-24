package flights.producer;

import flights.parser.AirportParser;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import radar.AirportUpdateEvent;

public class AirportProducer extends Producer {
    private static final Logger logger = LogManager.getLogger(AirportProducer.class);

    public AirportProducer(Properties configuration) {
        super(configuration);
        this.topic = configuration.getProperty("kafka.topic.airport.update.events");
        this.inputResourceName = configuration.getProperty("airports.input.resource");
    }

    @Override
    public void run() {

        List<AirportUpdateEvent> events = AirportParser.parse(inputResourceName);
        for (AirportUpdateEvent airport : events) {
            this.producer.send(
                    new ProducerRecord<>(this.topic, airport.getCode().toString(), airport));
            logger.info(String.format(
                    "Published event with code %s to %s", airport.getCode().toString(), topic));
        }
    }
}
