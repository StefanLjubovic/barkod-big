package flights.parser;

import flights.producer.AirportProducer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import radar.AirportUpdateEvent;

public class AirportParser {
    private static final Logger logger = LogManager.getLogger(AirportProducer.class);
    private static final String delimiter = ",";

    public static List<AirportUpdateEvent> parse(String resourceName) {

        URL inputSource = Thread.currentThread().getContextClassLoader().getResource(resourceName);
        List<AirportUpdateEvent> airports = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(inputSource.openStream()))) {
            String line;
            while ((line = br.readLine()) != null) airports.add(toAirport(line));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return airports;
    }

    private static AirportUpdateEvent toAirport(String line) {
        AirportUpdateEvent airport = new AirportUpdateEvent();
        String[] values = line.split(delimiter);

        airport.setCode(values[0]);
        airport.setAirpot(values[1]);
        airport.setCity(values[2]);
        airport.setCountry(values[3]);
        airport.setLatitude(Double.parseDouble(values[4]));
        airport.setLongitude(Double.parseDouble(values[5]));
        airport.setTz(values[6]);
        logger.info(String.format(
                "Parsed airport update event with code %s ", airport.getCode().toString()));

        return airport;
    }
}
