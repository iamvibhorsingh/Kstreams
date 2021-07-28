package com.learning.kafkastreaming.chapter6;


import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


/****************************************************************************
 * This is an example for Website Views Analytics in Kafka Streams.
 * It reads a real time views from Kafka, computes 5 second user summaries
 * and keeps track of a leaderboard for topics with maximum views
 ****************************************************************************/


public class WebsiteViewsAnalytics {

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

        //Initiate the Kafka Views Generator
        KafkaViewsDataGenerator viewsGenerator = new KafkaViewsDataGenerator();
        Thread genThread = new Thread(viewsGenerator);
        genThread.start();

        System.out.println("******** Starting Streaming  *************");

        try {
            /**************************************************
             * Build a Kafka Topology
             **************************************************/

            //Setup Serializer / DeSerializer for used Data types
            final Serde<String> stringSerde = Serdes.String();
            final Serde<Long> longSerde = Serdes.Long();
            final Serde<WebsiteView> viewSerde
                    = Serdes.serdeFrom(new ClassSerializer<>(),
                            new ClassDeSerializer<>(WebsiteView.class));
            final Serde<ViewAggregator> aggregatorSerde
                    = Serdes.serdeFrom(new ClassSerializer<>(),
                    new ClassDeSerializer<>(ViewAggregator.class));

            //Setup Properties for the Kafka Input Stream
            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG,
                    "website-view-analytics-pipe");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                    "localhost:9092");
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                    Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                    Serdes.String().getClass());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

            //For immediate results during testing
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
            props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

            //Initiate the Kafka Streams Builder
            final StreamsBuilder builder = new StreamsBuilder();

            //Create the source node for Views
            KStream<String,String> viewsInput
                    = builder.stream("streaming.views.input",
                                        Consumed.with(
                                                stringSerde, stringSerde));

            ObjectMapper mapper = new ObjectMapper();

            //Convert input CSV to View Object
            KStream<String,WebsiteView> viewsObjects
                    = viewsInput.mapValues( (inputCSV) -> {
                String[] values = inputCSV
                        .replaceAll("\"","")
                        .split(",");
                WebsiteView view = new WebsiteView();
                view.setTimestamp(Timestamp.valueOf(values[0]));
                view.setUser(values[1]);
                view.setTopic(values[2]);
                view.setMinutes(Integer.valueOf(values[3]));
                System.out.println("Received View :" + view);
                return view;
            });

            //Create a time window of 5 seconds
            TimeWindows tumblingWindow = TimeWindows
                    .of(Duration.ofSeconds(5))
                    .grace(Duration.ZERO);

            //Initalize a new ViewAggregator every time a new key is found
            Initializer<ViewAggregator> viewAggregatorInitializer
                    = ViewAggregator::new;

            //Aggregate using the view aggregator.
            Aggregator<String, WebsiteView, ViewAggregator> viewAdder
                    = (key, value, aggregate)
                        -> aggregate.add(value.getMinutes());

            //Compute user wise, window wise total minutes
            KTable<Windowed<String>,ViewAggregator> userSummary
                    = viewsObjects
                    .groupBy(
                        (key,value) -> value.getUser() ,
                        Grouped.with(stringSerde,viewSerde))
                    .windowedBy(tumblingWindow)
                    .aggregate(
                            viewAggregatorInitializer,
                            viewAdder,
                            Materialized.<String, ViewAggregator,
                                    WindowStore<Bytes, byte[]>>as(
                                            "time-windowed-aggregate-store")
                                        .withValueSerde(aggregatorSerde))
                    .suppress(
                            Suppressed
                                    .untilWindowCloses(
                                    Suppressed.BufferConfig
                                            .unbounded()
                                            .shutDownWhenFull()));

            //Print summary to console
            userSummary
                    .toStream()
                    .peek( (key, aggregation) ->
                            {
                                System.out.println("Received Summary :" +
                                        " Window = " + key.window().startTime() +
                                        " User =" + key.key() +
                                        " Value = " + aggregation.getTotalValue());
                            }
                    );

            //Update redis leaderboard with topic views. Use count =1 for record.
            viewsObjects
                    .foreach((key,view)
                            -> redisUpdater.update_score(
                                    view.getTopic(), 1.0)
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

        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }

}
