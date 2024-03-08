package io.confluent.developer.joins;

import io.confluent.developer.StreamsUtils;
import io.confluent.developer.avro.ApplianceOrder;
import io.confluent.developer.avro.CombinedOrder;
import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.developer.avro.User;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StreamsJoin {

    // This static helper method for getting SerDes for the Avro Records.
    // This will be abstracted in a static utility method in the Udalls class of the course repo.
    // Utility Method that is used to load the properties. You can refer to the Udalls Class in the
    // Exercise source code. ./gradlew runStreams -Pargs=joins
    static <T extends SpecificRecord> SpecificAvroSerde<T> getSpecificAvroSerde(final Map<String, Object> serdeConfig) {
        final SpecificAvroSerde<T> specificAvroSerde = new SpecificAvroSerde<>();
        specificAvroSerde.configure(serdeConfig, false);
        return specificAvroSerde;
    }

    public static void main(String[] args) throws IOException {
        Properties streamsProps = StreamsUtils.loadProperties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "joining-streams");

        // Next get the input topic names and the output topic Name from the properties.
        StreamsBuilder builder = new StreamsBuilder();
        String streamOneInput = streamsProps.getProperty("stream_one.input.topic");
        String streamTwoInput = streamsProps.getProperty("stream_two.input.topic");
        String tableInput = streamsProps.getProperty("table.input.topic");
        String outputTopic = streamsProps.getProperty("joins.output.topic");

        // after creating a HashMap with the configs...
        Map<String, Object> configMap = StreamsUtils.propertiesToMap(streamsProps);

        // ...lets create the required SerDes for all streams and the table.
        SpecificAvroSerde<ApplianceOrder> applianceSerde = getSpecificAvroSerde(configMap);
        SpecificAvroSerde<ElectronicOrder> electronicSerde = getSpecificAvroSerde(configMap);
        SpecificAvroSerde<CombinedOrder> combinedSerde = getSpecificAvroSerde(configMap);
        SpecificAvroSerde<User> userSerde = getSpecificAvroSerde(configMap);

        // Now create the Value joiner for the Stream-Stream join by taking the left side
        // and the right side of the join to create a combine order object.
        ValueJoiner<ApplianceOrder, ElectronicOrder, CombinedOrder> orderJoiner =
                (applianceOrder, electronicOrder) -> CombinedOrder.newBuilder()
                        .setApplianceOrderId(applianceOrder.getOrderId())
                        .setApplianceId(applianceOrder.getApplianceId())
                        .setElectronicOrderId(electronicOrder.getOrderId())
                        .setTime(Instant.now().toEpochMilli())
                        .build();
        // Next create the ValueJoiner for the Stream Table Join
        // The Stream is a Result of the preceding Stream - Stream Join, but its a left outer join because the
        // right side record 'user' might not exist.
        ValueJoiner<CombinedOrder, User, CombinedOrder> enrichmentJoiner = (combined, user) -> {
            if (user != null) {
                combined.setUserName(user.getName());
            }
            return combined;
        };

        // Then create the appliance order stream...
        KStream<String, ApplianceOrder> applianceStream =
                builder.stream(streamOneInput, Consumed.with(Serdes.String(), applianceSerde))
                        .peek((key, value) -> System.out.println("Appliance stream incoming record key " + key + " value " + value));

        // ...as well as the electronic order stream.
        KStream<String, ElectronicOrder> electronicStream =
                builder.stream(streamTwoInput, Consumed.with(Serdes.String(), electronicSerde))
                        .peek((key, value) -> System.out.println("Electronic stream incoming record " + key + " value " + value));

        // From here create the user Table
        KTable<String, User> userTable =
                builder.table(tableInput, Materialized.with(Serdes.String(), userSerde));

        // And lastly the combined stream. Call the .join()-Method on the applianceStream as the
        // 'left side' or 'primary-stream' in the join. Add the electronic stream as the 'right side'
        // or 'secondary Stream' in the join.
        // Specify a join window of 30 minutes
        KStream<String, CombinedOrder> combinedStream =
                applianceStream.join( // the left side
                    electronicStream, // the right side
                    orderJoiner,
                    JoinWindows.of(Duration.ofMinutes(30)), // Join Window in Minutes
                    StreamJoined.with(Serdes.String(), applianceSerde, electronicSerde)) // StreamJoined config with Serdes for the key left side and ride side objects for join state stores.
                        .peek((key, value) -> System.out.println("Stream-Stream Join record key " + key + " value " + value)); // Views the Results of the join

        combinedStream.leftJoin( // the left side...
                        userTable, // ...and the right side
                        enrichmentJoiner, // Enrichtment Joiner for added easier information if available...
                        Joined.with(Serdes.String(), combinedSerde, userSerde)) // Add the joined config object with the SerDes for the values of both sides of the join
                .peek((key, value) -> System.out.println("Stream-Table Join record key " + key + " value " + value)) // Peek the results...
                .to(outputTopic, Produced.with(Serdes.String(), combinedSerde)); // And write the final join results to a topic.

        // create a Join between the applianceStream and the electronicStream
        // using the ValueJoiner created above, orderJoiner gets you the correct value type of CombinedOrder
        // You want to join records within 30 minutes of each other HINT: JoinWindows and Duration.ofMinutes
        // Add the correct Serdes for the join state stores remember both sides have same key type
        // HINT: StreamJoined and Serdes.String  and Serdes for the applianceStream and electronicStream created above

        // Optionally add this statement after the join to see the results on the console
        // .peek((key, value) -> System.out.println("Stream-Stream Join record key " + key + " value " + value));


        // Now join the combinedStream with the userTable,
        // but you'll always want a result even if no corresponding entry is found in the table
        // Using the ValueJoiner created above, enrichmentJoiner, return a CombinedOrder instance enriched with user information
        // You'll need to add a Joined instance with the correct Serdes for the join state store

        // Add these two statements after the join call to print results to the console and write results out
        // to a topic

        // .peek((key, value) -> System.out.println("Stream-Table Join record key " + key + " value " + value))
        // .to(outputTopic, Produced.with(Serdes.String(), combinedSerde));

        // Create the kafkaStreams object...
        try (KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps)) {
            final CountDownLatch shutdownLatch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                kafkaStreams.close(Duration.ofSeconds(2));
                shutdownLatch.countDown();
            }));
            TopicLoader.runProducer(); // Use the TopicLoader helper class to create topics and produce the exercise data.
            try {
                kafkaStreams.start(); // and start the Stream
                shutdownLatch.await();
            } catch (Throwable e) {
                System.exit(1);
            }
        }
        System.exit(0);
    }
}
