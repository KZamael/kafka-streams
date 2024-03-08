package io.confluent.developer.aggregate;

import io.confluent.developer.StreamsUtils;
import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

// Note: Make sure to let the application run for 30 plus seconds! ./gradlew runStreams -Pargs=aggregate
/**
 * Exercise is all about Streams Stateful Operations, specifically Aggregation.
 */
public class StreamsAggregate {

    public static void main(String[] args) throws IOException {

        final Properties streamsProps = StreamsUtils.loadProperties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregate-streams");

        StreamsBuilder builder = new StreamsBuilder();
        final String inputTopic = streamsProps.getProperty("aggregate.input.topic");
        final String outputTopic = streamsProps.getProperty("aggregate.output.topic");
        final Map<String, Object> configMap = StreamsUtils.propertiesToMap(streamsProps);

        // To begin we'll use utility methods for loading properties in getting specific record Avro serdes.
        final SpecificAvroSerde<ElectronicOrder> electronicSerde =
                StreamsUtils.getSpecificAvroSerde(configMap);

        // First you create the ElectronicOrder Stream.
        final KStream<String, ElectronicOrder> electronicStream =
                builder.stream(inputTopic, Consumed.with(Serdes.String(), electronicSerde))
                        .peek((key, value) -> System.out.println("Incoming record - key " + key + " value " + value));

        // Now take the electronicStream object, group by key (groupyByKey) and perform an aggregation (aggregate)
        // Don't forget to convert the KTable returned by the aggregate call back to a KStream using the toStream method
        electronicStream.groupByKey().aggregate(() -> 0.0,
                (key, order, total) -> total + order.getPrice(), // taking each order and adding the price to a running total sum of all electronic orders.
                Materialized.with(Serdes.String(), Serdes.Double())) // Now, add a Materialized in a Serde to provide state store Serdes as the value type has changed.
        // To view the results of the aggregation consider
        // right after the toStream() method .peek((key, value) -> System.out.println("Outgoing record - key " +key +" value " + value))
                .toStream()
                .peek((key, value) -> System.out.println("key " + key + " value " + value))
        // Finally write the results to an output topic
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Double())); // you want to add a to() operator to write results in a topic

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
