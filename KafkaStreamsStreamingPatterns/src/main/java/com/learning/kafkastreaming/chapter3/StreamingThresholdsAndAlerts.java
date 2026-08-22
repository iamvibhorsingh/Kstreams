package com.learning.kafkastreaming.chapter3;

import com.learning.kafkastreaming.common.ClassDeSerializer;
import com.learning.kafkastreaming.common.ClassSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/****************************************************************************
 * This is an example for Alerts & Thresholds in Kafka Streams.
 * It reads a real-time alerts stream from kafka,
 * compares against thresholds, and publishes alerts.
 ****************************************************************************/
public class StreamingThresholdsAndAlerts {

        private static final Logger log = LoggerFactory.getLogger(StreamingThresholdsAndAlerts.class);

        public static void main(String[] args) {

                // Initiate the Kafka Alerts Generator
                KafkaAlertsDataGenerator alertsGenerator = new KafkaAlertsDataGenerator();
                Thread genThread = new Thread(alertsGenerator);
                genThread.start();

                log.info("******** Starting Streaming Alerts and Thresholds *************");

                try {
                        /**************************************************
                         * Build a Kafka Topology
                         **************************************************/

                        final Serde<String> stringSerde = Serdes.String();
                        final Serde<Alert> alertSerde = Serdes.serdeFrom(new ClassSerializer<>(),
                                        new ClassDeSerializer<>(Alert.class));

                        Properties props = new Properties();
                        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "alerts-and-thresholds-pipe");
                        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
                        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

                        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
                        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

                        final StreamsBuilder builder = new StreamsBuilder();

                        // Source node for Alerts
                        KStream<String, String> alertInput = builder.stream("streaming.alerts.input",
                                        Consumed.with(stringSerde, stringSerde));

                        // Convert value to an Alert Object
                        KStream<String, Alert> alertObject = alertInput.mapValues(inputCSV -> {
                                String[] values = inputCSV.replaceAll("\"", "").split(",");
                                Alert alert1 = new Alert();
                                alert1.setTimestamp(Timestamp.valueOf(values[0]));
                                alert1.setLevel(values[1]);
                                alert1.setCode(values[2]);
                                alert1.setMesg(values[3]);
                                log.info("Received Alert : {}", alert1);
                                return alert1;
                        });

                        // Filter Critical Alerts and Publish to an outgoing topic
                        alertObject
                                        .filter((key, alert) -> "CRITICAL".equals(alert.getLevel()))
                                        .mapValues(alert -> "\"" + alert.getTimestamp() + "\",\"" + alert.getCode() + "\",\"" + alert.getMesg() + "\"")
                                        .to("streaming.alerts.critical", Produced.with(stringSerde, stringSerde));

                        // Tumbling window of 10 seconds
                        TimeWindows tumblingWindow = TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10));

                        // Aggregate by Code and window
                        KTable<Windowed<String>, Long> codeCounts = alertObject.groupBy(
                                        (key, value) -> value.getCode(),
                                        Grouped.with(stringSerde, alertSerde))
                                        .windowedBy(tumblingWindow)
                                        .count(Materialized.as("code-counts"))
                                        .suppress(Suppressed.untilWindowCloses(
                                                        Suppressed.BufferConfig.unbounded().shutDownWhenFull()));

                        codeCounts
                                        .toStream()
                                        .peek((key, value) -> log.info("Summary record : {} = {}", key, value))
                                        .filter((key, value) -> value > 2)
                                        .map((key, value) -> {
                                                String returnKey = key.toString();
                                                String returnVal = "\"" + key.window().startTime() + "\",\"" + key.key() + "\",\"" + value + "\"";
                                                log.info("High Volume Alert : {}", returnVal);
                                                return new KeyValue<>(returnKey, returnVal);
                                        })
                                        .to("streaming.alerts.highvolume", Produced.with(stringSerde, stringSerde));

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
                        log.error("Error in StreamingThresholdsAndAlerts", e);
                }
        }
}
