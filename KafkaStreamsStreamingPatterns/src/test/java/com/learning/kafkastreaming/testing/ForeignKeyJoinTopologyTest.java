package com.learning.kafkastreaming.testing;

import com.learning.kafkastreaming.common.JsonSerde;
import com.learning.kafkastreaming.joins.ForeignKeyJoin.Customer;
import com.learning.kafkastreaming.joins.ForeignKeyJoin.EnrichedOrder;
import com.learning.kafkastreaming.joins.ForeignKeyJoin.Order;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TableJoined;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Automated unit test validating KIP-213 Foreign Key Joins using TopologyTestDriver.
 */
class ForeignKeyJoinTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Order> ordersTopic;
    private TestInputTopic<String, Customer> customersTopic;
    private TestOutputTopic<String, EnrichedOrder> outputTopic;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();

        JsonSerde<Order> orderSerde = new JsonSerde<>(Order.class);
        JsonSerde<Customer> customerSerde = new JsonSerde<>(Customer.class);
        JsonSerde<EnrichedOrder> enrichedSerde = new JsonSerde<>(EnrichedOrder.class);

        KTable<String, Order> ordersTable = builder.table(
                "orders",
                Consumed.with(Serdes.String(), orderSerde),
                Materialized.<String, Order, KeyValueStore<Bytes, byte[]>>as("orders-test-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(orderSerde));

        KTable<String, Customer> customersTable = builder.table(
                "customers",
                Consumed.with(Serdes.String(), customerSerde),
                Materialized.<String, Customer, KeyValueStore<Bytes, byte[]>>as("customers-test-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(customerSerde));

        KTable<String, EnrichedOrder> enriched = ordersTable.join(
                customersTable,
                order -> order.customerId,
                (order, customer) -> new EnrichedOrder(
                        order.orderId,
                        order.amount,
                        customer != null ? customer.name : "UNKNOWN",
                        customer != null ? customer.tier : "STANDARD"
                ),
                TableJoined.as("fk-join-test"),
                Materialized.<String, EnrichedOrder, KeyValueStore<Bytes, byte[]>>as("enriched-test-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(enrichedSerde)
        );

        enriched.toStream().to("enriched-orders", Produced.with(Serdes.String(), enrichedSerde));

        Topology topology = builder.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fk-join-test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        testDriver = new TopologyTestDriver(topology, props);

        ordersTopic = testDriver.createInputTopic(
                "orders",
                Serdes.String().serializer(),
                orderSerde.serializer());

        customersTopic = testDriver.createInputTopic(
                "customers",
                Serdes.String().serializer(),
                customerSerde.serializer());

        outputTopic = testDriver.createOutputTopic(
                "enriched-orders",
                Serdes.String().deserializer(),
                enrichedSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    @DisplayName("Should successfully enrich order with customer data via foreign key")
    void testForeignKeyJoinEnrichment() {
        // 1. Insert customer
        customersTopic.pipeInput("cust-100", new Customer("cust-100", "Alice", "PLATINUM"));

        // 2. Insert order referencing cust-100
        ordersTopic.pipeInput("ord-001", new Order("ord-001", "cust-100", 250.0));

        Map<String, EnrichedOrder> results = outputTopic.readKeyValuesToMap();
        assertThat(results).containsKey("ord-001");
        EnrichedOrder enrichedOrder = results.get("ord-001");
        assertThat(enrichedOrder.orderId).isEqualTo("ord-001");
        assertThat(enrichedOrder.customerName).isEqualTo("Alice");
        assertThat(enrichedOrder.customerTier).isEqualTo("PLATINUM");
        assertThat(enrichedOrder.amount).isEqualTo(250.0);
    }
}
