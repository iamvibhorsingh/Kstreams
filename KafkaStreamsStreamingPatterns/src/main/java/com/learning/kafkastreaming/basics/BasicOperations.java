package com.learning.kafkastreaming.basics;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates basic stateless transformations in Kafka Streams.
 */
public class BasicOperations {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "basic-operations-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        // 1. Stream from input topic
        KStream<String, String> sourceStream = builder.stream("basics-input-topic");

        // 2. Peek - Observe records without modifying them (e.g. for debugging/logging)
        sourceStream.peek((key, value) -> System.out.println("Received: " + key + " = " + value))

                // 3. Filter - Keep only records where value length > 5
                .filter((key, value) -> value != null && value.length() > 5)

                // 4. MapValues - Transform values (e.g., to UPPERCASE)
                .mapValues(value -> value.toUpperCase())

                // 5. Map - Transform both key and value
                .map((key, value) -> new KeyValue<>(key != null ? key.toUpperCase() : "UNKNOWN", value))

                // 6. FlatMapValues - One to Many transformation (e.g., split sentence into
                // words)
                .flatMapValues(value -> Arrays.asList(value.split("\\W+")))

                // 7. SelectKey - Change the key of the record (e.g., make the word itself the
                // key)
                .selectKey((key, word) -> word)

                // 8. Write to output topic
                .to("basics-output-topic", Produced.with(Serdes.String(), Serdes.String()));

        // 9. Foreach - Terminal operation equivalent to peek + end of stream processing
        sourceStream
                .foreach((key, value) -> System.out.println("Processing completed for original stream record: " + key));

        Topology topology = builder.build();
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
}
