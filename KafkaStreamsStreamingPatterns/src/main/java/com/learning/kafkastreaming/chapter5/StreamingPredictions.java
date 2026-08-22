package com.learning.kafkastreaming.chapter5;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/****************************************************************************
 * This is an example for Streaming Predictions in Kafka Streams.
 * It reads real-time reviews from a Kafka topic
 * and uses an NLP/HTTP Service to predict sentiments and publish them.
 ****************************************************************************/
public class StreamingPredictions {

    private static final Logger log = LoggerFactory.getLogger(StreamingPredictions.class);

    public static void main(String[] args) {

        // Initiate the Kafka Reviews data Generator
        KafkaReviewsDataGenerator reviewsGenerator = new KafkaReviewsDataGenerator();
        Thread genThread = new Thread(reviewsGenerator);
        genThread.start();

        log.info("******** Starting Streaming Predictions *************");

        try {
            final Serde<String> stringSerde = Serdes.String();

            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "predictions-pipe");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
            props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

            final StreamsBuilder builder = new StreamsBuilder();

            KStream<String, String> reviewsInput = builder.stream("streaming.sentiment.input",
                    Consumed.with(stringSerde, stringSerde));

            reviewsInput.peek((key, review) -> log.info("Received Review : ID = {}, Review = {}", key, review));

            // Call the sentiment service for each record in the stream
            KStream<String, String> sentiments = reviewsInput.mapValues(review -> {
                String sentiment = SentimentPredictor.getSentiment(review);
                log.info("Output - Sentiment = {} : for {}", sentiment, review);
                return sentiment;
            });

            // Send to output topic
            sentiments.to("streaming.sentiment.output", Produced.with(stringSerde, stringSerde));

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
            log.error("Error in StreamingPredictions", e);
        }
    }
}
