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

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates custom partitioners and timestamp extractors.
 */
public class CustomPartitioner {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "custom-partitioner-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Define default timestamp extractor at the application level
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, PayloadTimestampExtractor.class.getName());

        StreamsBuilder builder = new StreamsBuilder();

        // The input topics will use the PayloadTimestampExtractor to define event time
        KStream<String, String> stream = builder.stream("partitioner-input-topic",
                Consumed.with(Serdes.String(), Serdes.String()));

        // Write to output using a custom stream partitioner
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
     * Custom Partitioner that routes records based on the first letter of the
     * value.
     */
    public static class FirstLetterPartitioner implements StreamPartitioner<String, String> {
        @Override
        public Integer partition(String topic, String key, String value, int numPartitions) {
            String sanitizedValue = value != null ? value.trim() : "";
            if (sanitizedValue.isEmpty()) {
                return 0; // default to partition 0
            }
            char firstChar = sanitizedValue.charAt(0);

            // Basic hashing logic over the first char
            return Math.abs((int) firstChar) % numPartitions;
        }
    }

    /**
     * Custom Timestamp Extractor to extract timestamp from the payload instead of
     * record metadata.
     */
    public static class PayloadTimestampExtractor implements TimestampExtractor {
        @Override
        public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
            // Assume the payload is a CSV: "timestampInMillis,event,..."
            String value = (String) record.value();
            if (value != null) {
                String[] parts = value.split(",");
                if (parts.length > 0) {
                    try {
                        return Long.parseLong(parts[0]);
                    } catch (NumberFormatException e) {
                        // ignore and fallback
                    }
                }
            }
            // Fallback to the partition time if payload does not have a timestamp
            return partitionTime;
        }
    }
}
