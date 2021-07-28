package com.learning.kafkastreaming.chapter4;


import com.learning.kafkastreaming.chapter3.Alert;
import com.learning.kafkastreaming.chapter3.KafkaAlertsDataGenerator;
import com.learning.kafkastreaming.common.ClassDeSerializer;
import com.learning.kafkastreaming.common.ClassSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/****************************************************************************
 * This is an example for Streaming Leaderboards in Kafka Streams.
 * It reads a player score changes from kafka
 * and maintains a running leaderboard
 ****************************************************************************/
public class StreamingLeaderboards {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.WARN);

        //Initiate  RedisTracker to print 5 sec leader positions
        RedisManager redisTracker = new RedisManager();
        redisTracker.setUp();
        Thread redisThread = new Thread(redisTracker);
        redisThread.start();

        //Initiate RedisUpdater to be used for updating the leaderboard
        RedisManager redisUpdater = new RedisManager();
        redisUpdater.setUp();

        //Initiate the Kafka Gaming data Generator
        KafkaGamingDataGenerator gamingGenerator = new KafkaGamingDataGenerator();
        Thread genThread = new Thread(gamingGenerator);
        genThread.start();

        System.out.println("******** Starting Streaming  *************");

        try {
            /**************************************************
             * Build a Kafka Topology
             **************************************************/

            //Setup Serializer / DeSerializer for used Data types
            final Serde<String> stringSerde = Serdes.String();
            final Serde<Long> longSerde = Serdes.Long();

            //Setup Properties for the Kafka Input Stream
            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG,
                    "leaderboards-pipe");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                    "localhost:9092");
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                    Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                    Serdes.String().getClass());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            //For immediate results during testing
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
            props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

            //Initiate the Kafka Streams Builder
            final StreamsBuilder builder = new StreamsBuilder();

            //Create the source node for Gaming data
            KStream<String, String> gamingInput
                    = builder.stream("streaming.leaderboards.input",
                    Consumed.with(
                            stringSerde, stringSerde));

            gamingInput
                    .peek( (player, score)
                        -> System.out.println("Received Score : Player = " +
                                        player + ", Score = " + score));

            //Update the Redis key with the new score increment
            gamingInput
                    .foreach((product,score)
                        -> redisUpdater.update_score(product,
                                    Double.valueOf(score))
                            );

            /**************************************************
             * Create a pipe and execute
             **************************************************/
            //Create final topology and print
            final Topology topology = builder.build();
            System.out.println(topology.describe());

            //Setup Stream
            final KafkaStreams streams = new KafkaStreams(topology, props);

            //Reset for the example. Not recommended for production
            streams.cleanUp();
            final CountDownLatch latch = new CountDownLatch(1);

            // attach shutdown handler to catch control-c
            Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
                @Override
                public void run() {
                    System.out.println("Shutdown called..");
                    streams.close();
                    latch.countDown();
                }
            });

            streams.start();
            latch.await();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
