package com.learning.kafkastreaming.joins;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates joining two KTables.
 * Table-Table joins are continuous non-windowed joins that represent the latest
 * state.
 */
public class TableTableJoin {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "table-table-join-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();

        // KTable of User Profiles
        KTable<String, String> userProfiles = builder.table("user-profiles",
                Consumed.with(Serdes.String(), Serdes.String()));

        // KTable of User Settings
        KTable<String, String> userSettings = builder.table("user-settings",
                Consumed.with(Serdes.String(), Serdes.String()));

        // Join two tables: The result is a new KTable reflecting the joined state.
        // Needs to be co-partitioned.
        KTable<String, String> joinedTable = userProfiles.join(
                userSettings,
                (profile, settings) -> "Profile=[" + profile + "], Settings=[" + settings + "]");

        // Also supports leftJoin and outerJoin
        userProfiles.leftJoin(
                userSettings,
                (profile, settings) -> "Profile=[" + profile + "], Settings=[" + (settings == null ? "None" : settings)
                        + "]");

        // Output changelog to a topic
        joinedTable.toStream().to("user-complete-state", Produced.with(Serdes.String(), Serdes.String()));

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
