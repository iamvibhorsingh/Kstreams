package com.learning.kafkastreaming.advanced;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates inspecting, reading, and propagating Kafka Record Headers
 * (such as distributed tracing IDs, tenant IDs, or schema metadata)
 * using the modern Kafka 3.x Processor API.
 */
public class HeaderPropagationProcessor {

    private static final Logger log = LoggerFactory.getLogger(HeaderPropagationProcessor.class);

    public static final String TRACE_HEADER = "x-trace-id";
    public static final String AUDIT_TIMESTAMP_HEADER = "x-processed-timestamp";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "header-propagation-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        Topology topology = new Topology();

        topology.addSource("Source", "headers-input-topic")
                .addProcessor("AuditProcessor",
                        (ProcessorSupplier<String, String, String, String>) AuditHeaderProcessor::new,
                        "Source")
                .addSink("Sink", "headers-output-topic", "AuditProcessor");

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

    public static class AuditHeaderProcessor implements Processor<String, String, String, String> {
        private ProcessorContext<String, String> context;

        @Override
        public void init(ProcessorContext<String, String> context) {
            this.context = context;
        }

        @Override
        public void process(Record<String, String> record) {
            // 1. Read existing headers
            Header traceHeader = record.headers().lastHeader(TRACE_HEADER);
            String traceId;
            if (traceHeader != null) {
                traceId = new String(traceHeader.value(), StandardCharsets.UTF_8);
            } else {
                // Generate a new trace ID if not present
                traceId = UUID.randomUUID().toString();
            }

            log.info("Processing record [Key: {}, Value: {}] with Trace-ID: {}",
                    record.key(), record.value(), traceId);

            // 2. Clone headers and attach audit information
            RecordHeaders updatedHeaders = new RecordHeaders(record.headers());
            updatedHeaders.remove(TRACE_HEADER);
            updatedHeaders.add(new RecordHeader(TRACE_HEADER, traceId.getBytes(StandardCharsets.UTF_8)));
            updatedHeaders.add(new RecordHeader(AUDIT_TIMESTAMP_HEADER,
                    String.valueOf(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8)));

            // 3. Create a new Record preserving/updating headers
            Record<String, String> forwardedRecord = record
                    .withHeaders(updatedHeaders)
                    .withValue(record.value() != null ? record.value().toUpperCase() : "");

            context.forward(forwardedRecord);
        }
    }
}
