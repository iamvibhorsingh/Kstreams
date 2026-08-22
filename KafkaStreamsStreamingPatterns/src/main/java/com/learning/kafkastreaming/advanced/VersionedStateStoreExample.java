package com.learning.kafkastreaming.advanced;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.query.VersionedKeyQuery;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.VersionedBytesStoreSupplier;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates Versioned Key-Value State Stores (KIP-889) introduced in Kafka Streams 3.5+.
 *
 * Traditional KeyValueStores only store the latest value for a key.
 * Versioned State Stores track record history per key over a configured retention window,
 * enabling point-in-time temporal queries ("What was the price/address of item X at timestamp T?")
 * via Interactive Queries v2 (VersionedKeyQuery).
 */
public class VersionedStateStoreExample {

    private static final Logger log = LoggerFactory.getLogger(VersionedStateStoreExample.class);
    private static final String STORE_NAME = "item-prices-versioned-store";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "versioned-store-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        Topology topology = new Topology();

        // 1. Create a versioned store supplier with a 7-day history retention
        Duration historyRetention = Duration.ofDays(7);
        VersionedBytesStoreSupplier supplier = Stores.persistentVersionedKeyValueStore(STORE_NAME, historyRetention);

        StoreBuilder<VersionedKeyValueStore<String, String>> storeBuilder =
                Stores.versionedKeyValueStoreBuilder(supplier, Serdes.String(), Serdes.String());

        // 2. Add source, processor, and state store to topology
        topology.addSource("PriceUpdatesSource", "price-updates-topic")
                .addProcessor("VersionedProcessor",
                        (ProcessorSupplier<String, String, String, String>) VersionedPricingProcessor::new,
                        "PriceUpdatesSource")
                .addStateStore(storeBuilder, "VersionedProcessor")
                .addSink("PriceUpdatesSink", "price-updates-output", "VersionedProcessor");

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

            // 3. Point-in-time Historical Query via Interactive Queries v2 (VersionedKeyQuery)
            new Thread(() -> {
                try {
                    while (streams.state() != KafkaStreams.State.RUNNING) {
                        Thread.sleep(200);
                    }

                    Instant queryAsOf = Instant.now().minus(Duration.ofHours(1));

                    // Explicitly typed VersionedKeyQuery
                    VersionedKeyQuery<String, String> keyQuery = VersionedKeyQuery.<String, String>withKey("SKU-12345").asOf(queryAsOf);

                    StateQueryResult<VersionedRecord<String>> queryResult = streams.query(
                            StateQueryRequest.inStore(STORE_NAME).withQuery(keyQuery)
                    );

                    QueryResult<VersionedRecord<String>> partitionResult = queryResult.getOnlyPartitionResult();
                    if (partitionResult != null && partitionResult.getResult() != null) {
                        VersionedRecord<String> historicRecord = partitionResult.getResult();
                        log.info("Price 1 hour ago for SKU-12345: {} (Timestamp: {}, ValidTo: {})",
                                historicRecord.value(), historicRecord.timestamp(), historicRecord.validTo());
                    } else {
                        log.info("No historical record found for SKU-12345 as of {}", queryAsOf);
                    }
                } catch (Exception e) {
                    log.warn("Query thread exception: {}", e.getMessage());
                }
            }).start();

            latch.await();
        } catch (Exception e) {
            System.exit(1);
        }
    }

    public static class VersionedPricingProcessor implements Processor<String, String, String, String> {
        private ProcessorContext<String, String> context;
        private VersionedKeyValueStore<String, String> versionedStore;

        @Override
        public void init(ProcessorContext<String, String> context) {
            this.context = context;
            this.versionedStore = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(Record<String, String> record) {
            if (record.key() != null && record.value() != null) {
                // Puts the value into the versioned store with the record's event timestamp
                long putTimestamp = record.timestamp();
                versionedStore.put(record.key(), record.value(), putTimestamp);
                log.info("Saved versioned price: Key={}, Value={}, Timestamp={}",
                        record.key(), record.value(), putTimestamp);
            }
            context.forward(record);
        }
    }
}
