package flights;

import flights.configuration.Configuration;
import flights.serde.Serde;
import flights.topology.TopologyBuilder;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.*;
import radar.AirportKpi;
import radar.AirportUpdateEvent;
import radar.FlightUpdateEvent;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;


@TestInstance(value =  TestInstance.Lifecycle.PER_CLASS)
public class FlightKpiTopologyTest extends TopologyTest {
    private static final Logger logger = LogManager.getLogger(FlightKpiTopologyTest.class);

    private static final String SCHEMA_REGISTRY_SCOPE = FlightKpiTopologyTest.class.getName();

    private TestInputTopic<String, FlightUpdateEvent> flightUpdateEventTopic;
    private TestInputTopic<String, AirportUpdateEvent> airportUpdateEventTopic;
    private TestOutputTopic<String, AirportKpi> airportKpiTopic;
    private TestOutputTopic<String, SpecificRecord> flightTopic;
    private TestOutputTopic<String, SpecificRecord> a;

    private KeyValueStore<Object, ValueAndTimestamp<Object>> airportStore;


    private Serdes.StringSerde stringSerde = Serde.stringSerde;
    private SpecificAvroSerde specificAvroSerde;

    protected FlightKpiTopologyTest() throws IOException {
        super();
    }

    @BeforeAll
    public void buildTopology() {
        this.topology = new TopologyBuilder(this.props).build();
    }

    @BeforeEach
    public void setup() throws IOException {
        logger.info("Build topology.");


         this.specificAvroSerde = Serde.specificSerde(SpecificRecordBase.class, this.props.getProperty("kafka.schema.registry.url"));

        logger.info("Setup test driver.");
        driver = new TopologyTestDriver(topology, Configuration.streamsProperties(this.props));

        createInputTopics(driver);
        createOutputTopics(driver);

        MockSchemaRegistry.getClientForScope(SCHEMA_REGISTRY_SCOPE);

    }

    @AfterEach
    void afterEach() {
        driver.close();
//        this.deleteStateDir();
        MockSchemaRegistry.dropScope(SCHEMA_REGISTRY_SCOPE);
    }

    @Override
    public void createInputTopics(TopologyTestDriver driver) {
        logger.info("Creating input topics.");

        this.flightUpdateEventTopic = driver.createInputTopic(
                this.props.getProperty("kafka.topic.flight.update.events"),
                this.stringSerde.serializer(), this.specificAvroSerde.serializer()
        );
        this.airportUpdateEventTopic = driver.createInputTopic(
                this.props.getProperty("kafka.topic.airport.update.events"),
                this.stringSerde.serializer(), this.specificAvroSerde.serializer()
        );
    }

    @Override
    public void createOutputTopics(TopologyTestDriver driver) {
        logger.info("Creating output topics.");

        this.airportKpiTopic = driver.createOutputTopic(
                this.props.getProperty("kafka.topic.radar.airports.kpi"),
                this.stringSerde.deserializer(), this.specificAvroSerde.deserializer()
        );
        this.flightTopic = this.driver.createOutputTopic(
                this.props.getProperty("kafka.topic.radar.flights"),
                this.stringSerde.deserializer(), this.specificAvroSerde.deserializer()
        );

        this.a = this.driver.createOutputTopic(
               "topic-test",
                this.stringSerde.deserializer(), this.specificAvroSerde.deserializer()
        );

    }

    public void publishFlightUpdateEvent(
            String id,
            String status,
            String destination,
            Long departureTimestamp,
            Long arrivalTimestamp
            ) {
        flightUpdateEventTopic.pipeInput(id, new FlightUpdateEvent(
                id,
                "2023-02-02",
                destination,
                departureTimestamp,
                arrivalTimestamp,
                "Europe/Belgrade->Europe/Vienna",
                status,
                "G02",
                "Austrian"
        ));
    }


    public void publishAirportUpdateEvent(String code) {
        airportUpdateEventTopic.pipeInput(code, new AirportUpdateEvent(
                "Nikola Tesla",
                "Belgrade",
                "Serbia",
                code,
                44.8125,
                20.4612,
                "Europe/Belgrade"
        ));
    }

    @Test
    public void shouldProduceFlightEvent() {
        publishFlightUpdateEvent("1", "LANDED", "Belgrade/Serbia(BEG)->Vienna/Austria(VIE)", 1677195109L, 1677195109L);

        Assertions.assertEquals("1", flightTopic.readKeyValue().key );
    }

    @Test
    public void shouldNotProduceFlightEvent() {
        publishFlightUpdateEvent("1","CANCELED", "Belgrade/Serbia(BEG)->Vienna/Austria(VIE)", 1677195109L, 1677195109L);

        Assertions.assertTrue(flightTopic.isEmpty());
    }

    @Test
    public void shouldTransformFlightEvent() {
        publishFlightUpdateEvent("1","SCHEDULED", "Belgrade/Serbia(BEG)->Vienna/Austria(VIE)",1677195109L, 1677195109L);
        List<Schema.Field> actualFields = flightTopic.readValue().getSchema().getFields();


        Assertions.assertEquals(14, actualFields.size());
        Assertions.assertTrue(actualFields.contains(new Schema.Field("id", Schema.create(Schema.Type.STRING))));
        Assertions.assertTrue(actualFields.contains(new Schema.Field("from", Schema.create(Schema.Type.STRING))));
        Assertions.assertTrue(actualFields.contains(new Schema.Field("to", Schema.create(Schema.Type.STRING))));
        Assertions.assertTrue(actualFields.contains(new Schema.Field("departureAirportCode", Schema.create(Schema.Type.STRING))));
        Assertions.assertTrue(actualFields.contains(new Schema.Field("arrivalAirportCode", Schema.create(Schema.Type.STRING))));
    }


    @Test
    public void shouldPublishAirportKpi() {
        logger.info("Populate airport global KTable.");
        publishAirportUpdateEvent("BEG");
        publishAirportUpdateEvent("VIE");

        publishFlightUpdateEvent(
                "1",
                "LANDED",
            "Belgrade/Serbia(BEG)->Vienna/Austria(VIE)",
            Instant.now().toEpochMilli(),
            1677195109L
        );
        driver.advanceWallClockTime(Duration.ofMinutes(2));
        Assertions.assertFalse(flightTopic.isEmpty());
        Assertions.assertEquals(1, flightTopic.getQueueSize());
        Assertions.assertFalse(airportKpiTopic.isEmpty());
        Assertions.assertEquals(1, airportKpiTopic.getQueueSize());

        KeyValue<String, AirportKpi> kpi = airportKpiTopic.readKeyValue();
        Assertions.assertEquals(kpi.key, "BEG");
    }

    @Test
    public void shouldNotPublishAirportKpi() {
        logger.info("Populate airport global KTable.");
        publishAirportUpdateEvent("BEG");

        publishFlightUpdateEvent(
                "1",
                "LANDED",
                "Vienna/Austria(VIE)->Belgrade/Serbia(BEG)",
                Instant.now().toEpochMilli(),
                1677195109L
        );
        Assertions.assertTrue(airportKpiTopic.isEmpty());
        Assertions.assertEquals(0, airportKpiTopic.getQueueSize());
    }

}
