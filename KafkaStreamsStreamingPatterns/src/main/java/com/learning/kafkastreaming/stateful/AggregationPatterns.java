package com.learning.kafkastreaming.stateful;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates aggregate with Initializer/Aggregator and Windowing Patterns.
 */
public class AggregationPatterns {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregation-patterns-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> userActions = builder.stream("user-actions",
                Consumed.with(Serdes.String(), Serdes.String()));

        // We want to concatenate actions done by each user into a single CSV string.
        Initializer<String> actionInitializer = () -> "";

        Aggregator<String, String, String> actionAggregator =
                // key, new value, existing aggregate
                (userId, newAction, aggregatedActions) -> {
                    if (aggregatedActions.isEmpty()) {
                        return newAction;
                    } else {
                        return aggregatedActions + "," + newAction;
                    }
                };

        // 1. Global Aggregation (no window)
        KTable<String, String> totalActions = userActions
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(actionInitializer, actionAggregator, Materialized.with(Serdes.String(), Serdes.String()));

        // 2. Session Windows
        // Groups periods of activity separated by gaps of inactivity (e.g., 5 minutes)
        KTable<Windowed<String>, String> sessionActions = userActions
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(SessionWindows.with(Duration.ofMinutes(5)))
                .aggregate(actionInitializer, actionAggregator,
                        // When using session windows, we also need a Session Merger
                        (sessionKey, aggOne, aggTwo) -> aggOne + "," + aggTwo,
                        Materialized.with(Serdes.String(), Serdes.String()));

        // 3. Hopping Windows
        // 5 minute windows that advance/hop every 1 minute
        userActions
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(5)).advanceBy(Duration.ofMinutes(1)))
                .count(Materialized.as("hopping-window-counts"));

        totalActions.toStream().to("total-actions-output", Produced.with(Serdes.String(), Serdes.String()));

        // Output Windowed KTable to stream
        sessionActions.toStream()
                .selectKey((windowedKey, value) -> windowedKey.key() + "@" + windowedKey.window().start())
                .to("session-actions-output", Produced.with(Serdes.String(), Serdes.String()));

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
