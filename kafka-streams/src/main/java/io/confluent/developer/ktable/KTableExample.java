package io.confluent.developer.ktable;

import io.confluent.developer.ktable.TopicLoader;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

// Let this run for 40 seconds and you should see one result output: The latest update for the event
// with a given key /gradlew runStreams -Pargs=ktable
/**
 * Example for the usage of a KTable
 */
public class KTableExample {

    public static void main(String[] args) throws IOException {
        final Properties streamsProps = new Properties();
        try (FileInputStream fis = new FileInputStream("src/main/resources/streams.properties")) {
            streamsProps.load(fis);
        }
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-application");

        StreamsBuilder builder = new StreamsBuilder();
        final String inputTopic = streamsProps.getProperty("ktable.input.topic");
        final String outputTopic = streamsProps.getProperty("ktable.output.topic");

        // Create a variable to store the string that we want to filter on.
        final String orderNumberStart = "orderNumber-";

        // Crate a table with the StreamBuilder from above and use the table method
        // along with the inputTopic create a Materialized instance and name the store
        // and provide a Serdes for the key and the value  HINT: Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as
        // then use two methods to specify the key and value serde
        // KTable instance (notice that you call builder.table, instead of builder.stream):
        KTable<String, String> firstKTable = builder.table(
                // With the Materialized config object here, you need to provide a name for the KTable here
                // in order for it to be materialized. It will use caching and only admit the latest records
                // for each key after a commit, which is 30 seconds or when the cache is full at 10 megabytes.
            inputTopic, Materialized.<String, String, KeyValueStore<Bytes, byte[]>> as("ktable-store")
                        // add a Serde for the key...
                        .withKeySerde(Serdes.String())
                        // Serde for the value...
                        .withValueSerde(Serdes.String())
        );

        // Next we add a filter operator, removing records that do not contain the order number variable value.
        firstKTable.filter((key, value) -> value.contains(orderNumberStart))
                // Next you map the values by taking a substring
                        .mapValues(value -> value.substring(value.indexOf("-") + 1 ))
                        // Then you filter again taking out records, where the number of value of string is less than or equal to 1000.
                        .filter((key, value) -> Long.parseLong(value) > 1000)
                        // here you want to convert the KTable aka the Update Stream to a KStream aka event Stream!
                        .toStream()
                        // Add a peek operation to view the key values from the table
                        .peek((key, value) -> System.out.println("Outgoing record - key " + key + ", value " + value))
                        // Finally, write the records to a topic!
                        .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        // Now create the KafkaSteams Object...
        try (KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps)) {
            final CountDownLatch shutdownLatch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                kafkaStreams.close(Duration.ofSeconds(2));
                shutdownLatch.countDown();
            }));
            TopicLoader.runProducer();
            try {
                kafkaStreams.start();
                shutdownLatch.await();
            } catch (Throwable e) {
                System.exit(1);
            }
        }
        System.exit(0);
    }
}
