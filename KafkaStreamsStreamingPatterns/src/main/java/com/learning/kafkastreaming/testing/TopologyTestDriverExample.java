package com.learning.kafkastreaming.testing;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

/**
 * Demonstrates how to write an automated test for a Kafka Streams application
 * using TopologyTestDriver. This allows testing without a running Kafka
 * cluster.
 */
public class TopologyTestDriverExample {

    public static void main(String[] args) {

        System.out.println("Starting TopologyTestDriver Example...");

        // 1. Build the Topology to be tested
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("test-input-topic", Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(value -> value.toUpperCase())
                .filter((key, value) -> value.contains("KAFKA"))
                .to("test-output-topic", Produced.with(Serdes.String(), Serdes.String()));

        Topology topology = builder.build();

        // 2. Setup Properties
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"); // Not actually used by test driver
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // 3. Initialize the TopologyTestDriver
        try (TopologyTestDriver testDriver = new TopologyTestDriver(topology, props)) {

            // 4. Create Test Topics
            TestInputTopic<String, String> inputTopic = testDriver.createInputTopic(
                    "test-input-topic",
                    Serdes.String().serializer(),
                    Serdes.String().serializer());

            TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic(
                    "test-output-topic",
                    Serdes.String().deserializer(),
                    Serdes.String().deserializer());

            // 5. Pipe in test data
            System.out.println("Piping input data...");
            inputTopic.pipeInput("key1", "hello kafka streams");
            inputTopic.pipeInput("key2", "hello world"); // Should be filtered out
            inputTopic.pipeInput("key3", "learning KAFKA today");

            // 6. Assert outputs
            System.out.println("Reading output back...");

            KeyValue<String, String> record1 = outputTopic.readKeyValue();
            System.out.println("Record 1: " + record1.key + " -> " + record1.value);
            // Expected: key1 -> HELLO KAFKA STREAMS

            KeyValue<String, String> record2 = outputTopic.readKeyValue();
            System.out.println("Record 2: " + record2.key + " -> " + record2.value);
            // Expected: key3 -> LEARNING KAFKA TODAY

            if (outputTopic.isEmpty()) {
                System.out.println("Test Passed: No more output records expected.");
            } else {
                System.out.println("Test Failed: Unexpected records found in output topic.");
            }
        }
    }
}
