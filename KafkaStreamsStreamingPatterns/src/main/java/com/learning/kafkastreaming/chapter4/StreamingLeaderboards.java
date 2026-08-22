package com.learning.kafkastreaming.chapter4;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/****************************************************************************
 * This is an example for Streaming Leaderboards in Kafka Streams.
 * It reads player score changes from kafka
 * and maintains a running leaderboard in Redis.
 ****************************************************************************/
public class StreamingLeaderboards {

    private static final Logger log = LoggerFactory.getLogger(StreamingLeaderboards.class);

    public static void main(String[] args) {

        // Initiate RedisTracker to print 5 sec leader positions
        RedisManager redisTracker = new RedisManager();
        redisTracker.setUp();
        Thread redisThread = new Thread(redisTracker);
        redisThread.start();

        // Initiate RedisUpdater to be used for updating the leaderboard
        RedisManager redisUpdater = new RedisManager();
        redisUpdater.setUp();

        // Initiate the Kafka Gaming data Generator
        KafkaGamingDataGenerator gamingGenerator = new KafkaGamingDataGenerator();
        Thread genThread = new Thread(gamingGenerator);
        genThread.start();

        log.info("******** Starting Streaming Leaderboards *************");

        try {
            final Serde<String> stringSerde = Serdes.String();

            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "leaderboards-pipe");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
            props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

            final StreamsBuilder builder = new StreamsBuilder();

            KStream<String, String> gamingInput = builder.stream("streaming.leaderboards.input",
                    Consumed.with(stringSerde, stringSerde));

            gamingInput.peek((player, score) -> log.info("Received Score : Player = {}, Score = {}", player, score));

            // Update the Redis key with the new score increment
            gamingInput.foreach((product, score) -> redisUpdater.update_score(product, Double.parseDouble(score)));

            final Topology topology = builder.build();
            log.info("\n{}", topology.describe());

            final KafkaStreams streams = new KafkaStreams(topology, props);
            streams.cleanUp();
            final CountDownLatch latch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
                @Override
                public void run() {
                    log.info("Shutdown called..");
                    streams.close();
                    latch.countDown();
                }
            });

            streams.start();
            latch.await();

        } catch (Exception e) {
            log.error("Error in StreamingLeaderboards", e);
        }
    }
}
