package com.learning.kafkastreaming.joins;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates joins between KStream and KTable/GlobalKTable.
 * Commonly used to enrich a stream of events with reference data.
 */
public class StreamTableJoin {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-table-join-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();

        // High-volume event stream (e.g., clicks)
        KStream<String, String> userClicks = builder.stream("user-clicks",
                Consumed.with(Serdes.String(), Serdes.String()));

        // User profile reference data partitioned exactly like user-clicks (same key)
        KTable<String, String> userProfiles = builder.table("user-profiles",
                Consumed.with(Serdes.String(), Serdes.String()));

        // Example: Global product lookup table. Data is replicated to all tasks,
        // allowing joins on non-key attributes.
        GlobalKTable<String, String> globalProducts = builder.globalTable("global-products",
                Consumed.with(Serdes.String(), Serdes.String()));

        // 1. KStream to KTable Join
        // Co-partitioning is required: the events and reference data must be
        // partitioned on the same key.
        KStream<String, String> enrichedClicks = userClicks.leftJoin(
                userProfiles,
                (clickData, profileData) -> "Click: " + clickData + " by User: "
                        + (profileData != null ? profileData : "Unknown"),
                Joined.with(Serdes.String(), Serdes.String(), Serdes.String()));

        // 2. KStream to GlobalKTable Join
        // Can be joined on ANY attribute via the KeyValueMapper, no co-partitioning
        // required.
        KStream<String, String> enrichedWithProducts = enrichedClicks.leftJoin(
                globalProducts,
                (userId, enrichedData) -> extractProductId(enrichedData), // KeyValueMapper extracting the foreign key
                (enrichedData, productData) -> enrichedData + " -> Product: "
                        + (productData != null ? productData : "Unknown") // ValueJoiner
        );

        enrichedWithProducts.to("fully-enriched-clicks", Produced.with(Serdes.String(), Serdes.String()));

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

    private static String extractProductId(String data) {
        // Dummy implementation. In real code this would parse JSON/Avro.
        return "example-product-id";
    }
}
