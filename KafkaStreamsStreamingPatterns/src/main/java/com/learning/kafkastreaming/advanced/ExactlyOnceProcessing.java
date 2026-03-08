package com.learning.kafkastreaming.advanced;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates how to configure Kafka Streams for Exactly-Once Semantics (EOS).
 */
public class ExactlyOnceProcessing {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "eos-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Enable Exactly-Once Semantic (EOS)
        // In Kafka Streams 2.6+, EXACTLY_ONCE (eos-v1) or EXACTLY_ONCE_BETA (eos-v2) is
        // used.
        // In Kafka Streams 3.0+, EXACTLY_ONCE_V2 is the standard.
        // This configuration automatically sets idempotent producers, transactional
        // IDs, and read_committed isolation level.
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        // Optional: reduce commit interval for faster testing (default for EOS is
        // 100ms)
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> eosStream = builder.stream("eos-input-topic",
                Consumed.with(Serdes.String(), Serdes.String()));

        // Exactly-once applies to both state stores (RocksDB) and output records.
        eosStream.mapValues(value -> value.toUpperCase())
                .to("eos-output-topic", Produced.with(Serdes.String(), Serdes.String()));

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
