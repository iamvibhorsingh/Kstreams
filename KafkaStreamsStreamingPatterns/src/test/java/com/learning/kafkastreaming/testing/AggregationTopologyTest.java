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
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Automated unit test validating tumbling window aggregations and state store state.
 */
class AggregationTopologyTest {

    private static final String STORE_NAME = "window-count-store";
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();

        TimeWindows window = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1));

        builder.stream("page-views-topic", Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(window)
                .count(Materialized.as(STORE_NAME))
                .toStream()
                .selectKey((Windowed<String> key, Long value) -> key.key() + "@" + key.window().start())
                .to("window-counts-topic", Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = builder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "agg-test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        testDriver = new TopologyTestDriver(topology, props);

        inputTopic = testDriver.createInputTopic(
                "page-views-topic",
                Serdes.String().serializer(),
                Serdes.String().serializer());

        outputTopic = testDriver.createOutputTopic(
                "window-counts-topic",
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
    @DisplayName("Should aggregate counts per 1-minute window and persist into window store")
    void testWindowAggregation() {
        Instant baseTime = Instant.parse("2026-01-01T00:00:00Z");

        // Two events in the first window [00:00:00 - 00:01:00]
        inputTopic.pipeInput("userA", "view_home", baseTime.plusSeconds(5));
        inputTopic.pipeInput("userA", "view_cart", baseTime.plusSeconds(25));

        // One event in the second window [00:01:00 - 00:02:00]
        inputTopic.pipeInput("userA", "view_checkout", baseTime.plusSeconds(70));

        // Verify state store direct query
        ReadOnlyWindowStore<String, Long> windowStore = testDriver.getWindowStore(STORE_NAME);
        WindowStoreIterator<Long> iterator = windowStore.fetch("userA", baseTime, baseTime.plusSeconds(120));

        assertThat(iterator.hasNext()).isTrue();
        KeyValue<Long, Long> firstWindow = iterator.next();
        assertThat(firstWindow.value).isEqualTo(2L);

        assertThat(iterator.hasNext()).isTrue();
        KeyValue<Long, Long> secondWindow = iterator.next();
        assertThat(secondWindow.value).isEqualTo(1L);

        assertThat(iterator.hasNext()).isFalse();
    }
}
