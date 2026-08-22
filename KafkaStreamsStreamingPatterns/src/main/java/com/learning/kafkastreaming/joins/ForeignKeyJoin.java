package com.learning.kafkastreaming.joins;

import com.learning.kafkastreaming.common.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TableJoined;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates Foreign Key Table-to-Table Joins (KIP-213) in Kafka Streams.
 *
 * Joins two KTables where the join key is NOT the primary key of both tables,
 * but rather an attribute (foreign key) of the left table.
 *
 * Example: Orders Table (Key: orderId, Value: {orderId, customerId, amount})
 *          joined with
 *          Customers Table (Key: customerId, Value: {customerId, name, email, tier})
 */
public class ForeignKeyJoin {

    private static final Logger log = LoggerFactory.getLogger(ForeignKeyJoin.class);

    // Domain models
    public static class Order {
        public String orderId;
        public String customerId;
        public double amount;

        public Order() {}
        public Order(String orderId, String customerId, double amount) {
            this.orderId = orderId;
            this.customerId = customerId;
            this.amount = amount;
        }
    }

    public static class Customer {
        public String customerId;
        public String name;
        public String tier;

        public Customer() {}
        public Customer(String customerId, String name, String tier) {
            this.customerId = customerId;
            this.name = name;
            this.tier = tier;
        }
    }

    public static class EnrichedOrder {
        public String orderId;
        public double amount;
        public String customerName;
        public String customerTier;

        public EnrichedOrder() {}
        public EnrichedOrder(String orderId, double amount, String customerName, String customerTier) {
            this.orderId = orderId;
            this.amount = amount;
            this.customerName = customerName;
            this.customerTier = customerTier;
        }

        @Override
        public String toString() {
            return "EnrichedOrder{" +
                    "orderId='" + orderId + '\'' +
                    ", amount=" + amount +
                    ", customerName='" + customerName + '\'' +
                    ", customerTier='" + customerTier + '\'' +
                    '}';
        }
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "foreign-key-join-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        JsonSerde<Order> orderSerde = new JsonSerde<>(Order.class);
        JsonSerde<Customer> customerSerde = new JsonSerde<>(Customer.class);
        JsonSerde<EnrichedOrder> enrichedOrderSerde = new JsonSerde<>(EnrichedOrder.class);

        // 1. Orders KTable: Keyed by orderId
        KTable<String, Order> ordersTable = builder.table(
                "orders-topic",
                Consumed.with(Serdes.String(), orderSerde),
                Materialized.<String, Order, KeyValueStore<Bytes, byte[]>>as("orders-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(orderSerde));

        // 2. Customers KTable: Keyed by customerId
        KTable<String, Customer> customersTable = builder.table(
                "customers-topic",
                Consumed.with(Serdes.String(), customerSerde),
                Materialized.<String, Customer, KeyValueStore<Bytes, byte[]>>as("customers-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(customerSerde));

        // 3. Foreign Key Join:
        // Join ordersTable with customersTable by extracting order.customerId as the FK
        KTable<String, EnrichedOrder> enrichedOrders = ordersTable.join(
                customersTable,
                order -> order.customerId, // ForeignKeyExtractor: extracts customerId from Order
                (order, customer) -> new EnrichedOrder(
                        order.orderId,
                        order.amount,
                        customer != null ? customer.name : "UNKNOWN",
                        customer != null ? customer.tier : "STANDARD"
                ), // ValueJoiner
                TableJoined.as("orders-customers-fk-join"),
                Materialized.<String, EnrichedOrder, KeyValueStore<Bytes, byte[]>>as("enriched-orders-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(enrichedOrderSerde)
        );

        // 4. Output the continuously enriched changelog stream
        enrichedOrders.toStream()
                .peek((orderId, enriched) -> log.info("Enriched Order: {} -> {}", orderId, enriched))
                .to("enriched-orders-topic", Produced.with(Serdes.String(), enrichedOrderSerde));

        Topology topology = builder.build();
        log.info("Topology:\n{}", topology.describe());

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
