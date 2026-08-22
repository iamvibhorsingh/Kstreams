package com.learning.kafkastreaming.basics;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates modern stream splitting (branching) using split().branch() (KIP-632)
 * and stream merging.
 */
public class BranchAndMerge {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "branch-merge-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> sourceStream = builder.stream("branching-input-topic",
                Consumed.with(Serdes.String(), Serdes.String()));

        // 1. Fluent Branching with split() (replaces deprecated branch(Predicate...))
        Map<String, KStream<String, String>> branches = sourceStream.split(Named.as("route-"))
                .branch((key, value) -> value != null && value.startsWith("A"), Branched.as("A"))
                .branch((key, value) -> value != null && value.startsWith("B"), Branched.as("B"))
                .defaultBranch(Branched.as("default"));

        KStream<String, String> aStream = branches.get("route-A");
        KStream<String, String> bStream = branches.get("route-B");
        KStream<String, String> defaultStream = branches.get("route-default");

        // 2. Route distinct branches to individual topics
        aStream.to("topic-A", Produced.with(Serdes.String(), Serdes.String()));
        bStream.to("topic-B", Produced.with(Serdes.String(), Serdes.String()));
        defaultStream.to("topic-default", Produced.with(Serdes.String(), Serdes.String()));

        // 3. Merging branches back together
        KStream<String, String> mergedStream = aStream.merge(bStream);

        // Repartition on key transformation
        mergedStream
                .selectKey((key, value) -> value != null && !value.isEmpty() ? value.substring(0, 1) : "UNKNOWN")
                .to("topic-merged", Produced.with(Serdes.String(), Serdes.String()));

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
