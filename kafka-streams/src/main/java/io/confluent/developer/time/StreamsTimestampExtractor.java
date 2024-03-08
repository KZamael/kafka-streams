package io.confluent.developer.time;

import io.confluent.developer.StreamsUtils;
import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.logging.log4j.core.config.Order;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/** In this solution, well focus on the streams code and adding a timestamp-extractor ./gradlew runStreams -pArgs=time **/
public class StreamsTimestampExtractor {

    static class OrderTimestampExtractor implements TimestampExtractor {
        @Override
        public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
            // Extract the timestamp from the value field in the ConsumerRecord
            ElectronicOrder order = (ElectronicOrder) record.value();
            // and return that timestamp embedded in the electronic order
            return order.getTime();
        }
    }

    public static void main(String[] args) throws IOException {

        final Properties streamsProps = StreamsUtils.loadProperties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "extractor-windowed-streams");

        StreamsBuilder builder = new StreamsBuilder();
        final String inputTopic = streamsProps.getProperty("extractor.input.topic");
        final String outputTopic = streamsProps.getProperty("extractor.output.topic");
        final Map<String, Object> configMap = StreamsUtils.propertiesToMap(streamsProps);

        final SpecificAvroSerde<ElectronicOrder> electronicSerde =
                StreamsUtils.getSpecificAvroSerde(configMap);

        // Go ahead and create the KStream...
        final KStream<String, ElectronicOrder> electronicStream =
                // Make the now familiar builder.stream call...
                builder.stream(inputTopic,
                                // Add the Consumed config option with the SerDes for deserialization
                                Consumed.with(Serdes.String(), electronicSerde)
                        //Wire up the timestamp extractor HINT do it on the Consumed object vs configs
                                        .withTimestampExtractor(new OrderTimestampExtractor())) // local to this Stream only variant,
                        // if i wanted it globally for all Streams, i would have specified the timestamp via configs
                        .peek((key, value) -> System.out.println("Incoming record - key " + key + " value " + value));

        // Create the window aggregation with tumbling windows. Bear in mind that the timestamps from the electronic order are what
        // drive the action in terms of the window opening and closing.
        electronicStream.groupByKey().windowedBy(TimeWindows.of(Duration.ofHours(1)))
                //Now calling the aggregate method and initialize the aggregation to zero
                .aggregate(() -> 0.0,
                        // and adding the aggregator instance that sums all prices for the total spent over one hour
                        // based of the timestamp of the record itself
                        (key, order, total) -> total + order.getPrice(),
                        // from here add SerDes for the Materialized, which is the State Store...
                        Materialized.with(Serdes.String(), Serdes.Double()))
                // and convert the KTable from the aggregation into a KStream
                .toStream()
                // then you want a map processor to unwrap the window key and return the underlying key
                // of the aggregation.
                .map((wk, value) -> KeyValue.pair(wk.key(), value))
                // Next, add a peek processor to print the aggregation results to the console
                .peek((key, value) -> System.out.println("Outgoing record - key " + key + " value " + value))
                // Finally, write the results out to a topic
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Double()));

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
