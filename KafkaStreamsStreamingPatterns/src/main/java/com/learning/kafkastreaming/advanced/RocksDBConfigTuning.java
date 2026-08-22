package com.learning.kafkastreaming.advanced;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates production tuning of embedded RocksDB state stores using RocksDBConfigSetter.
 *
 * In high-throughput Kafka Streams applications, default RocksDB memory allocations
 * can cause JVM off-heap memory pressure, compaction stalls, or excessive disk I/O.
 *
 * Customizing RocksDBConfigSetter allows exact control over block caches, write buffers,
 * compression algorithms, and background threads.
 */
public class RocksDBConfigTuning {

    private static final Logger log = LoggerFactory.getLogger(RocksDBConfigTuning.class);

    /**
     * Custom RocksDB Configuration Setter.
     */
    public static class CustomRocksDBConfigSetter implements RocksDBConfigSetter {

        @Override
        public void setConfig(String storeName, Options options, Map<String, Object> configs) {
            log.info("Configuring RocksDB options for state store: {}", storeName);

            // 1. Block Cache Sizing (e.g. 64 MB shared block cache)
            org.rocksdb.TableFormatConfig tableFormatConfig = options.tableFormatConfig();
            if (tableFormatConfig instanceof BlockBasedTableConfig) {
                BlockBasedTableConfig blockBasedTableConfig = (BlockBasedTableConfig) tableFormatConfig;
                blockBasedTableConfig.setBlockCache(new LRUCache(64 * 1024 * 1024L)); // 64 MB
                blockBasedTableConfig.setBlockSize(16 * 1024L); // 16 KB block size
                blockBasedTableConfig.setCacheIndexAndFilterBlocks(true);
                blockBasedTableConfig.setPinL0FilterAndIndexBlocksInCache(true);
                options.setTableFormatConfig(blockBasedTableConfig);
            }

            // 2. MemTable / Write Buffer Configuration
            options.setWriteBufferSize(32 * 1024 * 1024L); // 32 MB write buffer
            options.setMaxWriteBufferNumber(3); // 3 buffers before write stall
            options.setMinWriteBufferNumberToMerge(1);

            // 3. Compression & Compaction Settings
            options.setCompressionType(CompressionType.LZ4_COMPRESSION);
            options.setCompactionStyle(CompactionStyle.LEVEL);
            options.setMaxBackgroundJobs(4); // Background flush & compaction threads
            options.setIncreaseParallelism(4);
        }

        @Override
        public void close(String storeName, Options options) {
            // Clean up custom native resources if instantiated
        }
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "rocksdb-tuning-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Register the custom RocksDB configuration setter class
        props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, CustomRocksDBConfigSetter.class.getName());

        StreamsBuilder builder = new StreamsBuilder();

        // State store created here will automatically use the CustomRocksDBConfigSetter
        builder.stream("high-throughput-input", Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.as("tuned-rocksdb-store"));

        Topology topology = builder.build();
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
            latch.await();
        } catch (Exception e) {
            System.exit(1);
        }
    }
}
