package com.learning.kafkastreaming.joins;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates Windowed Joins between two KStreams.
 * Stream-Stream joins are always windowed.
 */
public class StreamStreamJoin {

        public static void main(String[] args) {
                Properties props = new Properties();
                props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-stream-join-app");
                props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

                StreamsBuilder builder = new StreamsBuilder();

                KStream<String, String> leftStream = builder.stream("left-stream",
                                Consumed.with(Serdes.String(), Serdes.String()));
                KStream<String, String> rightStream = builder.stream("right-stream",
                                Consumed.with(Serdes.String(), Serdes.String()));

                // We want to join records from left stream and right stream that occur within 5
                // minutes of each other.
                JoinWindows joinWindow = JoinWindows.of(Duration.ofMinutes(5));

                // 1. Inner Join
                KStream<String, String> innerJoined = leftStream.join(
                                rightStream,
                                (leftValue, rightValue) -> "Left=" + leftValue + ", Right=" + rightValue, // ValueJoiner
                                joinWindow,
                                StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String()));

                // 2. Left Join
                leftStream.leftJoin(
                                rightStream,
                                (leftValue, rightValue) -> "Left=" + leftValue + ", Right="
                                                + (rightValue == null ? "NULL" : rightValue),
                                joinWindow,
                                StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())); // 3. Outer Join
                leftStream.outerJoin(
                                rightStream,
                                (leftValue, rightValue) -> "Left=" + (leftValue == null ? "NULL" : leftValue) +
                                                ", Right=" + (rightValue == null ? "NULL" : rightValue),
                                joinWindow,
                                StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String()));
                innerJoined.to("inner-join-output", Produced.with(Serdes.String(), Serdes.String()));

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
