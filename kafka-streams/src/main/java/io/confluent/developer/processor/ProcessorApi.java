package io.confluent.developer.processor;

import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static io.confluent.developer.StreamsUtils.*;

/** Implementation of a processor supplier and a processor instance, in which the processor will
 *  contain all the stream processing logic. ./gradlew runStreams -Pargs=processor
 */
public class ProcessorApi {

    // First Step: Create this Processor Supplier implementation.
    static class TotalPriceOrderProcessorSupplier implements ProcessorSupplier<String, ElectronicOrder, String, Double> {
        final String storeName;

        // Next Step: add a constructor...
        public TotalPriceOrderProcessorSupplier(String storeName) {
            this.storeName = storeName;
        }

        // Returns a new Processor instance each time get() is called.
        @Override
        public Processor<String, ElectronicOrder, String, Double> get() {
            return new Processor<>() {
                // declaring a variable for its context...
                private ProcessorContext<String, Double> context;
                // declaring a variable for the key value store...
                private KeyValueStore<String, Double> store;

                // Implememnting the init method
                @Override
                public void init(ProcessorContext<String, Double> context) {
                    // Save reference to the processor context
                    // Retrieve the store and save a reference
                    // Schedule a punctuation  HINT: use context.schedule and the method you want to call is forwardAll
                    this.context = context;
                    // get the store by name and store it in the variable declared earlier
                    store = context.getStateStore(storeName);
                    // Here you're using the processor context to schedule a punctuation to fire every 30 seconds based on stream time
                    // and you implement the forwardAll method.
                    this.context.schedule(Duration.ofSeconds(30), PunctuationType.STREAM_TIME, this::forwardAll);
                }

                private void forwardAll(final long timestamp) {
                    // Get a KeyValueIterator HINT there's a method on the KeyValueStore
                    // Don't forget to close the iterator! HINT use try-with resources
                    // Iterate over the records and create a Record instance and forward downstream HINT use a method on the ProcessorContext to forward

                    // First, you open an iterator for all the records in the store
                    try (KeyValueIterator<String, Double> iterator = store.all()) { // needs to be closed afterwards, best to use in a try with resources block so it'll close automatically
                        while (iterator.hasNext()) {
                            // inside the loop, you'll get a key value from the iterator.
                            final KeyValue<String, Double> nextKV = iterator.next();
                            // next you'll create a record instance
                            final Record<String, Double> totalPriceRecord = new Record<>(nextKV.key, nextKV.value, timestamp);
                            // finally, you'll forward the record to any of the child nodes
                            context.forward(totalPriceRecord);
                        }
                    }
                }

                // Then you implement the process method on the processor interface.
                @Override
                public void process(Record<String, ElectronicOrder> record) {
                    // Get the current total from the store HINT: use the key on the record
                    // Don't forget to check for null
                    // Add the price from the value to the current total from store and put it in the store
                    // HINT state stores are key-value stores
                    // get the key record
                    final String key = record.key();
                    // then use the key to see if there's a value in the state store
                    Double currentTotal = store.get(key);
                    // if its null, initialize it to zero
                    if (currentTotal == null) {
                        currentTotal = 0.0;
                    }
                    // Next add the current price from the record to the total
                    Double newTotal = record.value().getPrice() + currentTotal;
                    // Finally place the new value in the store with the given key
                    store.put(key, newTotal);
                }
            };
        }

        // Sore Builder is created, now override the Stores method on the processor interface,
        // which gives the processor access to the store
        @Override
        public Set<StoreBuilder<?>> stores() {
            return Collections.singleton(totalPriceStoreBuilder);
        }
    }


    // After the process method implementation, you'll define a storeName variable
    final static String storeName = "total-price-store";
    static StoreBuilder<KeyValueStore<String, Double>> totalPriceStoreBuilder = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(storeName), // create a Store and add the Name defined earlier
            // Next you'll add SerDes for the key and value types of the store.
            // Kafka-Streams stores everything as byte Arrays in the state source.
            Serdes.String(),
            Serdes.Double());

    public static void main(String[] args) throws IOException {
        final Properties streamsProps = loadProperties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-api-application");

        final String inputTopic = streamsProps.getProperty("processor.input.topic");
        final String outputTopic = streamsProps.getProperty("processor.output.topic");
        final Map<String, Object> configMap = propertiesToMap(streamsProps);

        final SpecificAvroSerde<ElectronicOrder> electronicSerde = getSpecificAvroSerde(configMap);
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Double> doubleSerde = Serdes.Double();


        final Topology topology = new Topology();

        // Add a source node to the topology  HINT: topology.addSource
        // Give it a name, add deserializers for the key and the value and provide the input topic name
        // Build the topology for the Streaming Application - now this takes a few more steps since its the Processor API
        // and not the streams DSL.
        topology.addSource(
            "source-node", // add a source node,
                stringSerde.deserializer(), // followed by the key deserializer
                electronicSerde.deserializer(), // ...and the value deserializer.
                inputTopic); // input topic name to complete the source node.

        // Now add a processor to the topology HINT topology.addProcessor
        // You'll give it a name for the processor, add a processor supplier HINT: a new instance and provide the store name
        // You'll also provide a parent name HINT: it's the name you used for the source node
        topology.addProcessor(
                "aggregate-price",
                    new TotalPriceOrderProcessorSupplier(storeName),
                "source-node" // parent name (or names) pointing to the source node above
                );
        // Finally, add a sink node HINT topology.addSink
        // As before give it a name, the output topic name, serializers for the key and value HINT: string and double
        // and the name of the parent node HINT it's the name you gave the processor
        topology.addSink(
                "sink-node",
                outputTopic, // name for the output topic
                stringSerde.serializer(), // key serializer
                doubleSerde.serializer(), // value serializer
                "aggregate price" // parent name (or names) for the sink node
        );

        // topology using the processor APi created! :)
        // Create KafkaStreams object, provide the topology and the propeties
        try (KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsProps)) {
            final CountDownLatch shutdownLatch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                kafkaStreams.close(Duration.ofSeconds(2));
                shutdownLatch.countDown();
            }));
            // Add the utility method that creates topics and provide sample data
            TopicLoader.runProducer();
            try {
                // Fire up the application! Timestamps are simulated, and the observed behaviour
                // could be different in a production environment.
                kafkaStreams.start();
                shutdownLatch.await();
            } catch (Throwable e) {
                System.exit(1);
            }
        }
        System.exit(0);
    }
}
