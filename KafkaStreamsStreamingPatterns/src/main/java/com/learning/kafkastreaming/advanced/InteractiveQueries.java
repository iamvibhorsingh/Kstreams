package com.learning.kafkastreaming.advanced;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.Arrays;
import java.util.Properties;

/**
 * Demonstrates Interactive Queries to read state store data directly from the
 * running streams app.
 */
public class InteractiveQueries {

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "interactive-queries-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        String storeName = "counts-store";

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("interactive-query-input", Consumed.with(Serdes.String(), Serdes.String()))
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .groupBy((key, word) -> word, Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.as(storeName));

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);

        streams.start();

        // Wait until streams is in RUNNING state
        while (streams.state() != KafkaStreams.State.RUNNING) {
            Thread.sleep(100);
        }

        // Query the state store
        // In a real application, this would be exposed via a REST API to users
        ReadOnlyKeyValueStore<String, Long> keyValueStore = streams.store(
                StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));

        // Simulated querying thread
        new Thread(() -> {
            try {
                while (true) {
                    Thread.sleep(5000); // query every 5 seconds
                    Long count = keyValueStore.get("kafka");
                    System.out.println("Current count for word 'kafka' is: " + (count != null ? count : 0));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
