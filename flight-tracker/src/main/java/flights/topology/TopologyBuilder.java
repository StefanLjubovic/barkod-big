package flights.topology;

import flights.serde.Serde;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import radar.AirportOutput;
import radar.AirportUpdateEvent;
import radar.FlightUpdateEvent;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TopologyBuilder implements Serde {

    private Properties config;

    public TopologyBuilder(Properties properties) {
        this.config = properties;
    }

    private static final Logger logger = LogManager.getLogger(TopologyBuilder.class);

    public Topology build() {
        StreamsBuilder builder = new StreamsBuilder();
        String schemaRegistry = config.getProperty("kafka.schema.registry.url");

        KStream<String, FlightUpdateEvent> flightInputStream = builder.stream(
                config.getProperty("kafka.topic.flight.update.events"),
                Consumed.with(Serde.stringSerde, Serde.specificSerde(FlightUpdateEvent.class, schemaRegistry)));

        GlobalKTable<String, AirportUpdateEvent> airportTable = builder.globalTable(
                config.getProperty("kafka.topic.airport.update.events"),
                Consumed.with(Serde.stringSerde, Serde.specificSerde(AirportUpdateEvent.class, schemaRegistry)));
        KStream<String, AirportOutput> outputKStream=flightInputStream
                .filter((s, flightUpdateEvent) -> !flightUpdateEvent.getStatus().toString().equals("CANCELED"))
                .map((key, flight) -> {
                    AirportOutput airportOutput = new AirportOutput(); // create a new AirportOutput object

                    String[] destination = flight.getDestination().toString().split("->");
                    String toPlace = destination[1].substring(0, destination[1].indexOf("("));
                    String fromPlace = destination[0].substring(0, destination[0].indexOf("("));

                    airportOutput.setTo(toPlace);
                    airportOutput.setFrom(fromPlace);

                    airportOutput.setId(flight.getId());
                    airportOutput.setAirline(flight.getAirline());
                    airportOutput.setDate(flight.getDate());
                    airportOutput.setStatus(flight.getStatus());
                    airportOutput.setGate(flight.getGate());

                    airportOutput.setDepartureTimestamp(flight.getSTD());
                    airportOutput.setArrivalTimestamp(flight.getSTA());
                    airportOutput.setDuration(TimeUnit.MILLISECONDS.toMinutes(flight.getSTA()-flight.getSTD()));

                    Instant instant = Instant.ofEpochMilli(flight.getSTD());
                    Instant instant1 = Instant.ofEpochMilli(flight.getSTA());

                    String[] parts = flight.getTimezones().toString().split("->");

                    ZonedDateTime zonedDateTime = instant.atZone(ZoneId.of(parts[0]));
                    ZonedDateTime zonedDateTime1 = instant1.atZone(ZoneId.of(parts[1]));

                    String iso8601 = zonedDateTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                    String iso86011 = zonedDateTime1.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);

                    airportOutput.setDepartureDatetime(iso8601);
                    airportOutput.setArrivalDatetime(iso86011);

                    Pattern pattern = Pattern.compile("\\((.*?)\\).*\\((.*?)\\)");
                    Matcher m = pattern.matcher(flight.getDestination());
                    String departureCode = "";
                    String arrivalCode = "";

                    if(m.find()) {
                        departureCode = m.group(1);
                        arrivalCode = m.group(2);
                    }
                    airportOutput.setDepartureAirportCode(departureCode);
                    airportOutput.setArrivalAirportCode(arrivalCode);


                    return KeyValue.pair(key, airportOutput);
                });;
        outputKStream
                .to("radar.flights", Produced.with(Serde.stringSerde,Serde.specificSerde(AirportOutput.class, schemaRegistry)));



        return builder.build();
    }
}
