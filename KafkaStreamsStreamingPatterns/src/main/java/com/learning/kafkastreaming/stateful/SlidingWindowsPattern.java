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
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates Sliding Window Aggregations (KIP-450) in Kafka Streams.
 *
 * Unlike fixed tumbling/hopping windows that align to epoch clock boundaries,
 * Sliding Windows evaluate continuous time intervals anchored on actual event arrival times.
 *
 * Use cases: Fraud detection (e.g. 5 failed logins within 2 minutes of each other),
 * burst traffic detection, moving rate calculations.
 */
public class SlidingWindowsPattern {

    private static final Logger log = LoggerFactory.getLogger(SlidingWindowsPattern.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sliding-windows-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> loginEvents = builder.stream(
                "login-events-topic",
                Consumed.with(Serdes.String(), Serdes.String()));

        // Continuous sliding window of 2 minutes:
        // Any two events within 2 minutes of each other are grouped together
        Duration timeDifference = Duration.ofMinutes(2);
        Duration gracePeriod = Duration.ofSeconds(30);
        SlidingWindows slidingWindow = SlidingWindows.ofTimeDifferenceAndGrace(timeDifference, gracePeriod);

        KTable<Windowed<String>, Long> slidingCounts = loginEvents
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(slidingWindow)
                .count(Materialized.as("sliding-login-counts-store"));

        slidingCounts.toStream()
                .peek((windowedKey, count) -> log.info("Sliding Window [{} to {}] User {} -> Events: {}",
                        windowedKey.window().startTime(),
                        windowedKey.window().endTime(),
                        windowedKey.key(),
                        count))
                // Flag potential security alerts if count exceeds threshold within the sliding window
                .filter((windowedKey, count) -> count >= 3)
                .mapValues((windowedKey, count) -> "SECURITY_ALERT: " + count + " attempts in 2m window for " + windowedKey.key())
                .selectKey((windowedKey, val) -> windowedKey.key())
                .to("login-alerts-topic", Produced.with(Serdes.String(), Serdes.String()));

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
