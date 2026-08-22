package com.learning.kafkastreaming.advanced;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates Kafka Streams modern exception handling, Dead Letter Queue (DLQ)
 * routing patterns, and StreamsUncaughtExceptionHandler thread recovery.
 */
public class ErrorHandling {

    private static final Logger log = LoggerFactory.getLogger(ErrorHandling.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "error-handling-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // 1. Deserialization Error Handling (e.g. corrupt or malformed messages)
        // LogAndContinueExceptionHandler logs the bad record and skips it without failing the task.
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> input = builder.stream("error-input-topic",
                Consumed.with(Serdes.String(), Serdes.String()));

        // 2. Dead Letter Queue (DLQ) Pattern for application processing errors
        // Instead of crashing the topology on poison-pill records, catch and route to DLQ
        Map<String, KStream<String, String>> branches = input.split(Named.as("process-"))
                .branch((key, value) -> {
                    try {
                        if (value != null && value.contains("BAD")) {
                            throw new IllegalArgumentException("Simulated processing error for bad value: " + value);
                        }
                        return true; // Success branch
                    } catch (Exception e) {
                        log.warn("Routing record with key '{}' to DLQ due to error: {}", key, e.getMessage());
                        return false;
                    }
                }, Branched.as("success"))
                .defaultBranch(Branched.as("dlq"));

        KStream<String, String> successStream = branches.get("process-success");
        KStream<String, String> dlqStream = branches.get("process-dlq");

        // Route clean records to main output
        successStream.to("success-output-topic", Produced.with(Serdes.String(), Serdes.String()));

        // Route malformed/error records to dead-letter-queue topic
        dlqStream.to("dead-letter-queue-topic", Produced.with(Serdes.String(), Serdes.String()));

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);

        // 3. Modern Uncaught Exception Handler (Kafka 2.8+)
        // Replaces deprecated Thread.UncaughtExceptionHandler with granular policy options
        streams.setUncaughtExceptionHandler(throwable -> {
            log.error("Fatal uncaught exception in Kafka Streams thread: ", throwable);
            // Options: REPLACE_THREAD (auto-heals thread), SHUTDOWN_CLIENT, SHUTDOWN_APPLICATION
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
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
