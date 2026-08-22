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
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Automated unit test validating that Suppressed.untilWindowCloses holds emissions
 * until stream time advances past the window.
 */
class WindowSuppressionTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();

        TimeWindows window = TimeWindows.ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(2));

        builder.stream("clicks-topic", Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(window)
                .count(Materialized.as("clicks-count-store"))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .selectKey((Windowed<String> key, Long value) -> key.key())
                .to("final-clicks-topic", Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = builder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "suppression-test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        testDriver = new TopologyTestDriver(topology, props);

        inputTopic = testDriver.createInputTopic(
                "clicks-topic",
                Serdes.String().serializer(),
                Serdes.String().serializer());

        outputTopic = testDriver.createOutputTopic(
                "final-clicks-topic",
                Serdes.String().deserializer(),
                Serdes.Long().deserializer());
    }

    @AfterEach
    void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    @DisplayName("Should suppress intermediate emissions and emit single final result when window closes")
    void testSuppressionUntilWindowCloses() {
        Instant t0 = Instant.parse("2026-01-01T12:00:00Z");

        // Pipe 3 records within window [12:00:00 - 12:00:10]
        inputTopic.pipeInput("user1", "click1", t0.plusSeconds(1));
        inputTopic.pipeInput("user1", "click2", t0.plusSeconds(3));
        inputTopic.pipeInput("user1", "click3", t0.plusSeconds(5));

        // Window is still open, nothing should be emitted yet
        assertThat(outputTopic.isEmpty()).isTrue();

        // Advance stream time past window end (10s) + grace (2s) by sending an event at 15s
        inputTopic.pipeInput("user1", "click4", t0.plusSeconds(15));

        // The first window should now be closed and emitted with count = 3
        List<KeyValue<String, Long>> emitted = outputTopic.readKeyValuesToList();
        assertThat(emitted).hasSize(1);
        assertThat(emitted.get(0).key).isEqualTo("user1");
        assertThat(emitted.get(0).value).isEqualTo(3L);
    }
}
