package com.learning.kafkastreaming.stateful;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates Window Suppression in Kafka Streams.
 *
 * In standard windowed aggregations, Kafka Streams emits an updated record
 * EVERY time an event arrives for an open window.
 *
 * Suppression (Suppressed.untilWindowCloses) buffers intermediate results in memory/changelog
 * and emits ONLY the final accumulated count when stream time advances past the window + grace period.
 */
public class WindowSuppression {

    private static final Logger log = LoggerFactory.getLogger(WindowSuppression.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "window-suppression-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> sensorReadings = builder.stream(
                "sensor-readings-topic",
                Consumed.with(Serdes.String(), Serdes.String()));

        // 1-minute tumbling window with 10-second grace period for late arrivals
        Duration windowSize = Duration.ofMinutes(1);
        Duration gracePeriod = Duration.ofSeconds(10);
        TimeWindows tumblingWindow = TimeWindows.ofSizeAndGrace(windowSize, gracePeriod);

        // Aggregate counts per sensor ID over the window
        KTable<Windowed<String>, Long> windowedCounts = sensorReadings
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(tumblingWindow)
                .count(Materialized.as("sensor-counts-store"))
                // SUPPRESSION: Hold emissions until stream-time moves past window_end + grace_period
                .suppress(Suppressed.untilWindowCloses(
                        Suppressed.BufferConfig.unbounded().shutDownWhenFull()));

        // Convert the suppressed KTable to a stream and output final aggregations
        windowedCounts.toStream()
                .peek((windowedKey, count) -> log.info("FINAL Window Result: Sensor {} [{} to {}] -> Total: {}",
                        windowedKey.key(),
                        windowedKey.window().startTime(),
                        windowedKey.window().endTime(),
                        count))
                .selectKey((windowedKey, count) -> windowedKey.key() + "@" + windowedKey.window().start())
                .to("final-sensor-metrics-topic", Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = builder.build();
        log.info("Topology:\n{}", topology.describe());

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
