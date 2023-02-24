package flights.generator;

import flights.parser.AirportParser;
import flights.utils.Utils;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;
import radar.AirportUpdateEvent;
import radar.FlightUpdateEvent;

public class Generator {
    private final List<AirportUpdateEvent> supportedAirports;

    private final int supportedAirportsSize;

    private final Random rand;

    public Generator(String resourceName) {
        this.supportedAirports = AirportParser.parse(resourceName);
        this.supportedAirportsSize = this.supportedAirports.size();
        this.rand = new Random(12);
    }

    public FlightUpdateEvent generateFlight() {
        FlightUpdateEvent flight = new FlightUpdateEvent();

        Map<String, AirportUpdateEvent> destination = generateDestination();
        LocalDateTime date = LocalDateTime.now();

        flight.setId(UUID.randomUUID().toString());
        flight.setDestination(formatDestination(destination));
        flight.setDate(date.toLocalDate().toString());
        flight.setSTD(Timestamp.valueOf(date).getTime());
        flight.setSTA(Utils.approximateArrivalTime(flight.getSTD(), destination));
        flight.setTimezones(formatTimezones(destination));
        flight.setGate(flight.getId().subSequence(0, 3));
        flight.setStatus(randomStatus());
        flight.setAirline(flight.getId().subSequence(0, 6));

        return flight;
    }

    private Map<String, AirportUpdateEvent> generateDestination() {
        int[] ids = rand.ints(2, 0, this.supportedAirportsSize).toArray();
        return new HashMap<String, AirportUpdateEvent>() {
            {
                put("start", supportedAirports.get(ids[0]));
                put("end", supportedAirports.get(ids[1]));
            }
        };
    }

    private String formatDestination(Map<String, AirportUpdateEvent> destination) {
        return String.format(
                "%s,%s(%s)->%s,%s(%s)",
                destination.get("start").getCity(),
                destination.get("start").getCountry(),
                destination.get("start").getCode(),
                destination.get("end").getCity(),
                destination.get("end").getCountry(),
                destination.get("end").getCode());
    }

    private String formatTimezones(Map<String, AirportUpdateEvent> destination) {
        return String.format(
                "%s->%s",
                destination.get("start").getTz(), destination.get("end").getTz());
    }

    private String randomStatus() {
        return FlightStatus.values()[this.rand.nextInt(FlightStatus.values().length)].toString();
    }
}
