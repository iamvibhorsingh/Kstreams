package com.learning.kafkastreaming.chapter5;


import com.learning.kafkastreaming.chapter4.KafkaGamingDataGenerator;
import com.learning.kafkastreaming.chapter4.RedisManager;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/****************************************************************************
 * This is an example for Streaming Predictions in Kafka Streams.
 * It reads real time reviews from a Kafka topic
 * and uses a HTTP Service to predict Sentiments and publish them.
 ****************************************************************************/
public class  StreamingPredictions {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.WARN);

        //Initiate the Kafka Reviews data Generator
        KafkaReviewsDataGenerator reviewsGenerator = new KafkaReviewsDataGenerator();
        Thread genThread = new Thread(reviewsGenerator);
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
                    "predictions-pipe");
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

            //Create the source node for Reviews data
            KStream<String, String> reviewsInput
                    = builder.stream("streaming.sentiment.input",
                    Consumed.with(
                            stringSerde, stringSerde));

            reviewsInput
                    .peek( (key, review)
                        -> System.out.println("Received Review : ID = " +
                                        key + ", Review = " + review));

            //Call the sentiment service for each record in the stream
            KStream<String,String> sentiments
                    = reviewsInput
                    .mapValues( ( review) -> {
                        String sentiment= SentimentPredictor.getSentiment(review);
                        System.out.println("Output - "
                                + "Sentiment= " + sentiment
                                + " : for " + review);
                        return sentiment;
                    });

            //Send to output topic
            sentiments
                    .to("streaming.sentiment.output");

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
            Runtime.getRuntime().addShutdownHook(
                    new Thread("streams-shutdown-hook") {
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
