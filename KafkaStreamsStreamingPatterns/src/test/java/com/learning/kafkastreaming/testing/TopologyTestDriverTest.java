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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Automated unit test for basic Kafka Streams transformations using JUnit 5 and TopologyTestDriver.
 */
class TopologyTestDriverTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("test-input-topic", Consumed.with(Serdes.String(), Serdes.String()))
                .filter((key, value) -> value != null && value.toLowerCase().contains("kafka"))
                .mapValues(value -> value.toUpperCase())
                .to("test-output-topic", Produced.with(Serdes.String(), Serdes.String()));

        Topology topology = builder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "unit-test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        testDriver = new TopologyTestDriver(topology, props);

        inputTopic = testDriver.createInputTopic(
                "test-input-topic",
                Serdes.String().serializer(),
                Serdes.String().serializer());

        outputTopic = testDriver.createOutputTopic(
                "test-output-topic",
                Serdes.String().deserializer(),
                Serdes.String().deserializer());
    }

    @AfterEach
    void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    @DisplayName("Should filter records not containing 'kafka' (case-insensitive) and uppercase matching values")
    void testFilterAndMapValues() {
        inputTopic.pipeInput("k1", "learning KAFKA streams");
        inputTopic.pipeInput("k2", "hello world"); // Should be filtered out
        inputTopic.pipeInput("k3", "kafka in action");

        List<KeyValue<String, String>> results = outputTopic.readKeyValuesToList();

        assertThat(results).hasSize(2);
        assertThat(results.get(0)).isEqualTo(new KeyValue<>("k1", "LEARNING KAFKA STREAMS"));
        assertThat(results.get(1)).isEqualTo(new KeyValue<>("k3", "KAFKA IN ACTION"));
        assertThat(outputTopic.isEmpty()).isTrue();
    }
}
