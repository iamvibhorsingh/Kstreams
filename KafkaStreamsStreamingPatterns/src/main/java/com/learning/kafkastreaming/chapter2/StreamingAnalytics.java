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
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/****************************************************************************
 * This is an example for Streaming Analytics in Kafka Streams.
 * It reads a real time orders stream from kafka, performs periodic summaries
 * and writes the output the a JDBC sink.
 ****************************************************************************/

public class StreamingAnalytics {

        private static final Logger log = LoggerFactory.getLogger(StreamingAnalytics.class);

        public static void main(String[] args) {

                // Initiate MariaDB DB Tracker and start the thread to print summaries every 5 seconds
                MariaDBManager dbTracker = new MariaDBManager();
                dbTracker.setUp();
                Thread dbThread = new Thread(dbTracker);
                dbThread.start();

                // Create another MariaDB Connection to update data
                MariaDBManager dbUpdater = new MariaDBManager();
                dbUpdater.setUp();

                // Initiate the Kafka Orders Generator
                KafkaOrdersDataGenerator ordersGenerator = new KafkaOrdersDataGenerator();
                Thread genThread = new Thread(ordersGenerator);
                genThread.start();

                log.info("******** Starting Streaming Analytics *************");

                try {
                        /**************************************************
                         * Build a Kafka Streams Topology
                         **************************************************/

                        final Serde<String> stringSerde = Serdes.String();
                        final Serde<SalesOrder> orderSerde = Serdes.serdeFrom(new ClassSerializer<>(),
                                        new ClassDeSerializer<>(SalesOrder.class));
                        final Serde<OrderAggregator> aggregatorSerde = Serdes.serdeFrom(new ClassSerializer<>(),
                                        new ClassDeSerializer<>(OrderAggregator.class));

                        Properties props = new Properties();
                        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streaming-analytics-pipe");
                        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
                        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

                        // Modern cache & commit interval configs
                        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
                        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

                        final StreamsBuilder builder = new StreamsBuilder();

                        // Source node for Orders
                        KStream<String, String> ordersInput = builder.stream("streaming.orders.input",
                                        Consumed.with(stringSerde, stringSerde));

                        ObjectMapper mapper = new ObjectMapper();

                        // Convert input JSON to SalesOrder object
                        KStream<String, SalesOrder> orderObjects = ordersInput.mapValues(inputJson -> {
                                try {
                                        return mapper.readValue(inputJson, SalesOrder.class);
                                } catch (Exception e) {
                                        log.error("Cannot convert JSON: {}", inputJson, e);
                                        return null;
                                }
                        });

                        orderObjects.peek((key, value) -> log.info("Received Order : {}", value));

                        // 5-second tumbling window with modern API
                        TimeWindows tumblingWindow = TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5));

                        Initializer<OrderAggregator> orderAggregatorInitializer = OrderAggregator::new;
                        Aggregator<String, SalesOrder, OrderAggregator> orderAdder = (key, value, aggregate) ->
                                (value == null || aggregate == null) ? aggregate : aggregate.add(value.getPrice() * value.getQuantity());

                        // Aggregation & Suppression
                        KTable<Windowed<String>, OrderAggregator> productSummary = orderObjects
                                        .filter((k, v) -> v != null)
                                        .groupBy((key, value) -> value.getProduct(), Grouped.with(stringSerde, orderSerde))
                                        .windowedBy(tumblingWindow)
                                        .aggregate(
                                                        orderAggregatorInitializer,
                                                        orderAdder,
                                                        Materialized.<String, OrderAggregator, WindowStore<Bytes, byte[]>>as(
                                                                        "time-windowed-aggregate-store")
                                                                        .withValueSerde(aggregatorSerde))
                                        .suppress(Suppressed.untilWindowCloses(
                                                        Suppressed.BufferConfig.unbounded().shutDownWhenFull()));

                        productSummary.toStream().foreach((key, aggregation) -> {
                                log.info("Received Summary : Window = {} Product = {} Value = {}",
                                                key.window().startTime(), key.key(), aggregation.getTotalValue());

                                dbUpdater.insertSummary(
                                                key.window().startTime().toString(),
                                                key.key(),
                                                aggregation.getTotalValue());
                        });

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
                        log.error("Fatal error in StreamingAnalytics", e);
                }
        }
}
