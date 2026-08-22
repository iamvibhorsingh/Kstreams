package com.learning.kafkastreaming.advanced;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates the modern Kafka Streams 3.x Processor API (KIP-405)
 * using the low-level Topology builder, typed Record<K, V>, and state stores.
 */
public class ProcessorApiExample {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-api-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        Topology topology = new Topology();

        // 1. Define a persistent key-value state store supplier
        StoreBuilder<KeyValueStore<String, Integer>> countStoreSupplier = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("CountsStore"),
                Serdes.String(),
                Serdes.Integer());

        // 2. Build the low-level topology: Source -> Processor -> Sink
        topology.addSource("Source", "processor-input-topic")
                .addProcessor("Process", (ProcessorSupplier<String, String, String, String>) WordCountProcessor::new, "Source")
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
     * Modern Kafka 3.x Processor implementation with typed Record<KIn, VIn>.
     */
    public static class WordCountProcessor implements Processor<String, String, String, String> {
        private ProcessorContext<String, String> context;
        private KeyValueStore<String, Integer> kvStore;

        @Override
        public void init(ProcessorContext<String, String> context) {
            this.context = context;
            this.kvStore = context.getStateStore("CountsStore");

            // Schedule a wall-clock periodic punctuator every 10 seconds
            this.context.schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
                System.out.println("--- Punctuation triggered at timestamp: " + timestamp + " ---");
                context.commit();
            });
        }

        @Override
        public void process(Record<String, String> record) {
            String value = record.value();
            if (value == null) return;

            String[] words = value.toLowerCase().split("\\W+");
            for (String word : words) {
                if (word.isEmpty()) continue;
                Integer oldCount = this.kvStore.get(word);
                this.kvStore.put(word, (oldCount == null ? 0 : oldCount) + 1);
            }

            // Forward the processed record downstream with new value
            Record<String, String> outputRecord = record.withValue(value + " [processed]");
            this.context.forward(outputRecord);
        }

        @Override
        public void close() {
            // Cleanup resources if needed
        }
    }
}
