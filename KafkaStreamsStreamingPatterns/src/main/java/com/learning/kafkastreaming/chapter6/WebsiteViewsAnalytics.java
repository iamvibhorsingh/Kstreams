package com.learning.kafkastreaming.chapter6;

import com.learning.kafkastreaming.common.ClassDeSerializer;
import com.learning.kafkastreaming.common.ClassSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/****************************************************************************
 * This is an example for Website Views Analytics in Kafka Streams.
 * It reads real-time views from Kafka, computes 5 second user summaries
 * and keeps track of a leaderboard for topics with maximum views.
 ****************************************************************************/

public class WebsiteViewsAnalytics {

        private static final Logger log = LoggerFactory.getLogger(WebsiteViewsAnalytics.class);

        public static void main(String[] args) {

                // Initiate RedisTracker to print 5 sec leader positions
                RedisManager redisTracker = new RedisManager();
                redisTracker.setUp();
                Thread redisThread = new Thread(redisTracker);
                redisThread.start();

                // Initiate RedisUpdater to be used for updating the leaderboard
                RedisManager redisUpdater = new RedisManager();
                redisUpdater.setUp();

                // Initiate the Kafka Views Generator
                KafkaViewsDataGenerator viewsGenerator = new KafkaViewsDataGenerator();
                Thread genThread = new Thread(viewsGenerator);
                genThread.start();

                log.info("******** Starting Streaming Website Views Analytics *************");

                try {
                        final Serde<String> stringSerde = Serdes.String();
                        final Serde<WebsiteView> viewSerde = Serdes.serdeFrom(new ClassSerializer<>(),
                                        new ClassDeSerializer<>(WebsiteView.class));
                        final Serde<ViewAggregator> aggregatorSerde = Serdes.serdeFrom(new ClassSerializer<>(),
                                        new ClassDeSerializer<>(ViewAggregator.class));

                        Properties props = new Properties();
                        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "website-view-analytics-pipe");
                        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
                        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

                        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
                        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

                        final StreamsBuilder builder = new StreamsBuilder();

                        // Source node for Views
                        KStream<String, String> viewsInput = builder.stream("streaming.views.input",
                                        Consumed.with(stringSerde, stringSerde));

                        // Convert input CSV to View Object
                        KStream<String, WebsiteView> viewsObjects = viewsInput.mapValues(inputCSV -> {
                                String[] values = inputCSV.replaceAll("\"", "").split(",");
                                WebsiteView view = new WebsiteView();
                                view.setTimestamp(Timestamp.valueOf(values[0]));
                                view.setUser(values[1]);
                                view.setTopic(values[2]);
                                view.setMinutes(Integer.parseInt(values[3]));
                                log.info("Received View : {}", view);
                                return view;
                        });

                        // 5-second tumbling window using modern API
                        TimeWindows tumblingWindow = TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5));

                        Initializer<ViewAggregator> viewAggregatorInitializer = ViewAggregator::new;
                        Aggregator<String, WebsiteView, ViewAggregator> viewAdder = (key, value, aggregate) ->
                                (value == null || aggregate == null) ? aggregate : aggregate.add(value.getMinutes());

                        // Compute user-wise windowed summary
                        KTable<Windowed<String>, ViewAggregator> userSummary = viewsObjects
                                        .filter((k, v) -> v != null)
                                        .groupBy((key, value) -> value.getUser(), Grouped.with(stringSerde, viewSerde))
                                        .windowedBy(tumblingWindow)
                                        .aggregate(
                                                        viewAggregatorInitializer,
                                                        viewAdder,
                                                        Materialized.<String, ViewAggregator, WindowStore<Bytes, byte[]>>as(
                                                                        "time-windowed-aggregate-store")
                                                                        .withValueSerde(aggregatorSerde))
                                        .suppress(Suppressed.untilWindowCloses(
                                                        Suppressed.BufferConfig.unbounded().shutDownWhenFull()));

                        userSummary.toStream().peek((key, aggregation) ->
                                log.info("Received Summary : Window = {} User = {} Value = {}",
                                                key.window().startTime(), key.key(), aggregation.getTotalValue()));

                        // Update Redis leaderboard with topic views
                        viewsObjects.filter((k, v) -> v != null).foreach((key, view) ->
                                redisUpdater.update_score(view.getTopic(), 1.0));

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
                        log.error("Error in WebsiteViewsAnalytics", e);
                }
        }
}
