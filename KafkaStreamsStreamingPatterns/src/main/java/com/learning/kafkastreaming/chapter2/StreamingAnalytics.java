package com.learning.kafkastreaming.chapter2;


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
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/****************************************************************************
 * This is an example for Streaming Analytics in Kafka Streams.
 * It reads a real time orders stream from kafka, performs periodic summaries
 * and writes the output the a JDBC sink.
 ****************************************************************************/


public class StreamingAnalytics {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.WARN);

        //Initiate MariaDB DB Tracker and start the thread to print
        //summaries every 5 seconds
        MariaDBManager dbTracker = new MariaDBManager();
        dbTracker.setUp();
        Thread dbThread = new Thread(dbTracker);
        dbThread.start();

        //Create another MariaDB Connection to update data
        MariaDBManager dbUpdater = new MariaDBManager();
        dbUpdater.setUp();

        //Initiate the Kafka Orders Generator
        KafkaOrdersDataGenerator ordersGenerator = new KafkaOrdersDataGenerator();
        Thread genThread = new Thread(ordersGenerator);
        genThread.start();

        System.out.println("******** Starting Streaming  *************");

        try {
            /**************************************************
             * Build a Kafka Streams Topology
             **************************************************/

            //Setup Serializer / DeSerializer for  Data types
            final Serde<String> stringSerde = Serdes.String();
            final Serde<Long> longSerde = Serdes.Long();

            final Serde<SalesOrder> orderSerde
                    = Serdes.serdeFrom(new ClassSerializer<>(),
                            new ClassDeSerializer<>(SalesOrder.class));
            final Serde<OrderAggregator> aggregatorSerde
                    = Serdes.serdeFrom(new ClassSerializer<>(),
                            new ClassDeSerializer<>(OrderAggregator.class));

            //Setup Properties for the Kafka Input Stream
            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG,
                    "streaming-analytics-pipe");
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

            //Create the source node for Orders
            KStream<String,String> ordersInput
                    = builder.stream("streaming.orders.input",
                                        Consumed.with(
                                                stringSerde, stringSerde));

            ObjectMapper mapper = new ObjectMapper();

            //Convert input json to SalesOrder object using Object Mapper
            KStream<String,SalesOrder> orderObjects
                    = ordersInput.mapValues(
                            new ValueMapper<String, SalesOrder>() {
                                @Override
                                public SalesOrder apply(String inputJson) {

                                    try {
                                        SalesOrder order =
                                                mapper.readValue(
                                                        inputJson,
                                                        SalesOrder.class);
                                        return order;
                                    }
                                    catch(Exception e) {
                                        System.out.println("ERROR : Cannot convert JSON "
                                                + inputJson);
                                        return null;
                                    }
                                }
                            }
                    );

            //Print objects received
            orderObjects.peek(
                    (key, value) ->
                            System.out.println("Received Order : " + value)
            );

            //Create a window of 5 seconds
            TimeWindows tumblingWindow = TimeWindows
                    .of(Duration.ofSeconds(5))
                    .grace(Duration.ZERO);

            //Initializer creates a new aggregator for every
            //Window & Product combination
            Initializer<OrderAggregator> orderAggregatorInitializer
                    = OrderAggregator::new;

            //Aggregator - Compute total value and call the aggregator
            Aggregator<String, SalesOrder, OrderAggregator> orderAdder
                    = (key, value, aggregate)
                        -> aggregate.add(value.getPrice()
                                        * value.getQuantity());

            //Perform Aggregation
            KTable<Windowed<String>,OrderAggregator> productSummary
                    = orderObjects
                    .groupBy( //Group by Product
                        (key,value) -> value.getProduct() ,
                        Grouped.with(stringSerde,orderSerde))
                    .windowedBy(tumblingWindow)
                    .aggregate(
                            orderAggregatorInitializer,
                            orderAdder,
                            //Store output in a materialized store
                            Materialized.<String, OrderAggregator,
                                    WindowStore<Bytes, byte[]>>as(
                                            "time-windowed-aggregate-store")
                                        .withValueSerde(aggregatorSerde))
                    .suppress(
                            Suppressed
                                    .untilWindowCloses(
                                    Suppressed.BufferConfig
                                            .unbounded()
                                            .shutDownWhenFull()));

            productSummary
                    .toStream() //convert KTable to KStream
                    .foreach( (key, aggregation) ->
                            {
                                System.out.println("Received Summary :" +
                                        " Window = " + key.window().startTime() +
                                        " Product =" + key.key() +
                                        " Value = " + aggregation.getTotalValue());

                                //Write to order_summary table
                                dbUpdater.insertSummary(
                                        key.window().startTime().toString(),
                                        key.key(),
                                        aggregation.getTotalValue()
                                        );
                            }
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

            //Start the stream
            streams.start();
            //Await termination
            latch.await();

        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }

}
