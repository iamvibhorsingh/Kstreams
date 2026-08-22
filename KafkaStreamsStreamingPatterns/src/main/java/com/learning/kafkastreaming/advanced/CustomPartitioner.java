package com.learning.kafkastreaming.advanced;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates custom partitioners and custom timestamp extractors in Kafka Streams 3.x.
 */
public class CustomPartitioner {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "custom-partitioner-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Define application-level default timestamp extractor
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, PayloadTimestampExtractor.class.getName());

        StreamsBuilder builder = new StreamsBuilder();

        // Input topic uses PayloadTimestampExtractor for event-time tracking
        KStream<String, String> stream = builder.stream("partitioner-input-topic",
                Consumed.with(Serdes.String(), Serdes.String()));

        // Sink records with custom stream partitioner
        stream.to(
                "partitioner-output-topic",
                Produced.with(Serdes.String(), Serdes.String(), new FirstLetterPartitioner()));

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

    /**
     * Modern StreamPartitioner implementation supporting both partitions and partition.
     */
    public static class FirstLetterPartitioner implements StreamPartitioner<String, String> {
        @Override
        public Optional<Set<Integer>> partitions(String topic, String key, String value, int numPartitions) {
            int p = partition(topic, key, value, numPartitions);
            return Optional.of(Collections.singleton(p));
        }

        @Override
        public Integer partition(String topic, String key, String value, int numPartitions) {
            if (value == null || value.trim().isEmpty()) {
                return 0;
            }
            char firstChar = value.trim().charAt(0);
            return Math.abs((int) firstChar) % numPartitions;
        }
    }

    /**
     * Custom Timestamp Extractor to extract timestamp embedded directly in the CSV payload.
     */
    public static class PayloadTimestampExtractor implements TimestampExtractor {
        @Override
        public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
            // Assume CSV format: "timestampInMillis,eventData,..."
            String value = (String) record.value();
            if (value != null) {
                String[] parts = value.split(",");
                if (parts.length > 0) {
                    try {
                        return Long.parseLong(parts[0].trim());
                    } catch (NumberFormatException ignored) {
                        // Fall through to partitionTime fallback
                    }
                }
            }
            return partitionTime;
        }
    }
}
