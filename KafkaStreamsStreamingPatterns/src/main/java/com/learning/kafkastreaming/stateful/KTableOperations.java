package com.learning.kafkastreaming.stateful;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates operations directly on KTable objects.
 */
public class KTableOperations {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-ops-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();

        // 1. Create a KTable directly from a compacted topic
        // KTable represents a changelog stream where each data record represents an
        // UPSERT.
        KTable<String, String> userProfiles = builder.table("user-profiles",
                Consumed.with(Serdes.String(), Serdes.String()));

        // 2. KTable transformations
        KTable<String, Integer> profileLengths = userProfiles
                // Filter: Drops records that do not match the predicate
                .filter((user, profileData) -> profileData != null && !profileData.isEmpty())
                // MapValues: Transform values (KTable does not support changing keys directly
                // via map)
                .mapValues(profileData -> profileData.length());

        // 3. GroupBy on KTable
        // Unlike KStream, grouping a KTable results in a KGroupedTable.
        // It takes care of subtracting the old aggregate and adding the new aggregate.
        KTable<Integer, Long> lengthCounts = profileLengths
                // Group KTable by value (creating a new key)
                .groupBy((user, length) -> org.apache.kafka.streams.KeyValue.pair(length, length),
                        Grouped.with(Serdes.Integer(), Serdes.Integer()))
                .count(Materialized.as("length-counts-store"));

        // 4. Output KTable to a changelog topic
        lengthCounts.toStream().to("length-counts-output", Produced.with(Serdes.Integer(), Serdes.Long()));

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
