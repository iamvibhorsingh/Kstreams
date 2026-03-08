package com.learning.kafkastreaming.advanced;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates Kafka Streams exception handling and Dead Letter Queue (DLQ)
 * patterns.
 */
public class ErrorHandling {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "error-handling-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // 1. Deserialization Error Handling (e.g. malformed JSON)
        // LogAndContinueExceptionHandler ignores the bad record and continues.
        // Default is LogAndFailExceptionHandler (which stops the stream).
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> input = builder.stream("error-input-topic");

        // 2. Dead Letter Queue Context (DLQ) pattern for processing errors
        // Instead of letting an exception crash the app, catch it and route to a DLQ.

        @SuppressWarnings("unchecked")
        KStream<String, String>[] branches = input.branch(
                (key, value) -> {
                    try {
                        // Simulating a processing step that might fail
                        if (value.contains("BAD")) {
                            throw new RuntimeException("Simulated processing error for value: " + value);
                        }
                        return true; // Success branch
                    } catch (Exception e) {
                        return false; // Error branch
                    }
                },
                (key, value) -> true // Fallback/Error branch
        );

        KStream<String, String> successStream = branches[0];
        KStream<String, String> errorStream = branches[1];

        // Route successful records to output
        successStream.to("success-output-topic");

        // Route failed records to dead-letter-queue
        errorStream.to("dead-letter-queue-topic");

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);

        // 3. Uncaught Exception Handler
        // Handled completely at the stream instance layer.
        streams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            System.err.println("Uncaught exception caught in " + thread.getName() + " - " + throwable.getMessage());
            // In 2.8+ you would return
            // StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD
        });

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
