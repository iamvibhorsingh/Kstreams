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

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates stateful operations: count and reduce on a KGroupedStream.
 */
public class CountAndReduce {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "count-reduce-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> lines = builder.stream("text-lines",
                Consumed.with(Serdes.String(), Serdes.String()));

        // 1. Counting
        // Word count example: flatMap string into words, group by word, and count
        KTable<String, Long> wordCounts = lines
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .groupBy((key, word) -> word, Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.as("word-counts-store"));

        // Output the counts to a topic
        wordCounts.toStream().to("word-counts-output", Produced.with(Serdes.String(), Serdes.Long()));

        // 2. Reducing
        // Example: Finding the longest string for a given key
        KStream<String, String> keyedLines = builder.stream("keyed-lines",
                Consumed.with(Serdes.String(), Serdes.String()));

        KTable<String, String> longestStrings = keyedLines
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .reduce(
                        // Reducer: returns the maximum length string between the current and the new
                        // one
                        (currentValue, newValue) -> newValue.length() > currentValue.length() ? newValue : currentValue,
                        // Materialize state to a local RocksDB store
                        Materialized.as("longest-string-store"));

        longestStrings.toStream().to("longest-string-output", Produced.with(Serdes.String(), Serdes.String()));

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
