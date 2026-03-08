package com.learning.kafkastreaming.advanced;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates the low-level Processor API using the Topology builder.
 */
public class ProcessorApiExample {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-api-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        Topology topology = new Topology();

        // Add a state store
        StoreBuilder<KeyValueStore<String, Integer>> countStoreSupplier = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("CountsStore"),
                Serdes.String(),
                Serdes.Integer());

        topology.addSource("Source", "processor-input-topic")
                .addProcessor("Process", () -> new WordCountProcessor(), "Source")
                .addStateStore(countStoreSupplier, "Process")
                .addSink("Sink", "processor-output-topic", "Process");

        KafkaStreams streams = new KafkaStreams(topology, props);

        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Exception e) {
            System.exit(1);
        }
    }

    /**
     * Custom low-level processor. Note: processor API changed in 3.0+, this uses
     * the 2.x API.
     */
    public static class WordCountProcessor implements Processor<String, String> {
        private ProcessorContext context;
        private KeyValueStore<String, Integer> kvStore;

        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context) {
            this.context = context;
            this.kvStore = (KeyValueStore<String, Integer>) context.getStateStore("CountsStore");

            // Schedule a punctuation every 10 seconds to emit total counts
            this.context.schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, new Punctuator() {
                @Override
                public void punctuate(long timestamp) {
                    System.out.println("--- Punctuation triggered at " + timestamp + " ---");
                    context.commit();
                }
            });
        }

        @Override
        public void process(String key, String value) {
            String[] words = value.toLowerCase().split("\\W+");

            for (String word : words) {
                Integer oldCount = this.kvStore.get(word);
                if (oldCount == null) {
                    this.kvStore.put(word, 1);
                } else {
                    this.kvStore.put(word, oldCount + 1);
                }
            }

            // Forward the result downstream
            this.context.forward(key, value + " processed");
        }

        @Override
        public void close() {
            // No-op
        }
    }
}
