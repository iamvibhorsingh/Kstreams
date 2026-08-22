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
 * Demonstrates Windowed Joins between two KStreams (Inner, Left, and Outer).
 * Stream-Stream joins are always windowed over event time.
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

        // Modern JoinWindows API (replaces deprecated JoinWindows.of(Duration))
        // Joins records that occur within +/- 5 minutes of each other in stream time
        JoinWindows joinWindow = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5));

        // 1. Inner Join: emits when both left and right records match within the window
        KStream<String, String> innerJoined = leftStream.join(
                rightStream,
                (leftValue, rightValue) -> "Left=" + leftValue + ", Right=" + rightValue,
                joinWindow,
                StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String()));

        // 2. Left Join: emits immediately for left records, enriched if right matches within window
        KStream<String, String> leftJoined = leftStream.leftJoin(
                rightStream,
                (leftValue, rightValue) -> "Left=" + leftValue + ", Right=" + (rightValue == null ? "NULL" : rightValue),
                joinWindow,
                StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String()));

        // 3. Outer Join: emits on either left or right event
        KStream<String, String> outerJoined = leftStream.outerJoin(
                rightStream,
                (leftValue, rightValue) -> "Left=" + (leftValue == null ? "NULL" : leftValue) +
                        ", Right=" + (rightValue == null ? "NULL" : rightValue),
                joinWindow,
                StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String()));

        innerJoined.to("inner-join-output", Produced.with(Serdes.String(), Serdes.String()));
        leftJoined.to("left-join-output", Produced.with(Serdes.String(), Serdes.String()));
        outerJoined.to("outer-join-output", Produced.with(Serdes.String(), Serdes.String()));

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
