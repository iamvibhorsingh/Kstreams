package com.learning.kafkastreaming.basics;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates stream splitting (branching) and merging.
 */
public class BranchAndMerge {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "branch-merge-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> sourceStream = builder.stream("branching-input-topic");

        // 1. Branching: splitting a stream into multiple streams based on predicates
        // NOTE: In Kafka Streams 2.8+, this array method is deprecated and replaced by
        // KStream.split().branch()
        @SuppressWarnings("unchecked")
        KStream<String, String>[] branches = sourceStream.branch(
                (key, value) -> value != null && value.startsWith("A"), // Index 0: 'A' words
                (key, value) -> value != null && value.startsWith("B"), // Index 1: 'B' words
                (key, value) -> true // Index 2: Default catch-all
        );

        KStream<String, String> aStream = branches[0];
        KStream<String, String> bStream = branches[1];
        KStream<String, String> defaultStream = branches[2];

        // 2. We can route distinct branches to different topics
        aStream.to("topic-A", Produced.with(Serdes.String(), Serdes.String()));
        bStream.to("topic-B", Produced.with(Serdes.String(), Serdes.String()));
        defaultStream.to("topic-default", Produced.with(Serdes.String(), Serdes.String()));

        // 3. Merging streams back together
        // bStream is merged back into aStream resulting in a single stream combining
        // both
        KStream<String, String> mergedStream = aStream.merge(bStream);

        // Let's repartition just to demonstrate (useful before aggregations where keys
        // changed)
        // Here we just use a selectKey to trigger internal repartitioning
        mergedStream
                .selectKey((key, value) -> value.substring(0, 1))
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
