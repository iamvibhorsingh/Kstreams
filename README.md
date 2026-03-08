# Comprehensive Kafka Streams Guide

This repository provides a comprehensive collection of **Kafka Streams** patterns and operations. It has been extended from a basic examples repository into a full guide covering everything from fundamental stateless operations to advanced topics like Exactly-Once Semantics (EOS), low-level Processor APIs, and Interactive Queries.

## 🚀 Examples Overview

All examples are standalone, runnable Java applications (`main` methods) organized by package under `src/main/java/com/learning/kafkastreaming/`.

### 1. Fundamentals (`basics` package)
- **`BasicOperations.java`**: Stateless transformations (`filter`, `map`, `flatMapValues`, `selectKey`, `peek`, `foreach`).
- **`BranchAndMerge.java`**: Routing a stream into multiple branches and merging them back together.

### 2. Stateful Operations (`stateful` package)
- **`CountAndReduce.java`**: Using `count` and `reduce` on a grouped stream.
- **`AggregationPatterns.java`**: Advanced aggregations with `Initializer` and `Aggregator`. Demonstrates Session Windows, Hopping Windows, and Global Aggregation.
- **`KTableOperations.java`**: Direct operations on a `KTable` (upsert stream) including `builder.table()`, filtering, and grouping a table.

### 3. Joins (`joins` package)
- **`StreamStreamJoin.java`**: Inner, Left, and Outer windowed joins between two `KStream`s.
- **`StreamTableJoin.java`**: `KStream` to `KTable` (co-partitioned) and `KStream` to `GlobalKTable` (broadcast) joins for stream enrichment.
- **`TableTableJoin.java`**: Continuous stateful joins between two `KTable`s.

### 4. Advanced Patterns (`advanced` package)
- **`ExactlyOnceProcessing.java`**: Configuring and using Kafka Streams' Exactly-Once Semantics (`processing.guarantee=exactly_once`).
- **`ErrorHandling.java`**: Handling deserialization exceptions (`LogAndContinue`) and routing runtime errors to a Dead Letter Queue (DLQ).
- **`ProcessorApiExample.java`**: Low-level `Processor` API with `Topology` builder. Demonstrates raw state store access and Wall-Clock Time `Punctuator` scheduling.
- **`InteractiveQueries.java`**: Exposing materialized state stores for direct querying from the outside using `ReadOnlyKeyValueStore`.
- **`CustomPartitioner.java`**: Writing a custom `StreamPartitioner` and `TimestampExtractor`.

### 5. Automated Testing (`testing` package)
- **`TopologyTestDriverExample.java`**: Unit testing Kafka Streams topologies without a real Kafka cluster using `TopologyTestDriver`, `TestInputTopic`, and `TestOutputTopic`.

### 6. Common Utilities (`common` package)
- **`JsonSerde.java`**: A highly reusable Generic JSON Serde built on Jackson.

---

### Legacy Real-World Use Cases (Chapters 2-6)
These are complete end-to-end applications demonstrating specific business scenarios:
- **Chapter 2 (Analytics):** Windowed aggregations writing 5-second summaries to a MariaDB JDBC sink.
- **Chapter 3 (Alerts):** Continuous threshold monitoring and filtering to identify anomalies.
- **Chapter 4 (Leaderboards):** Updating game scores in real-time and pushing to a Redis Sorted Set.
- **Chapter 5 (Predictions):** Enriching streams via external HTTP calls (Sentiment Prediction with NLP).
- **Chapter 6 (Views):** Keeping track of topics with maximum views using hopping windows and Redis.

## ⚙️ Setup and Prerequisites

### 1. Requirements
* Java 11 or higher
* Maven 3.6+
* A running Kafka cluster (ZooKeeper/KRaft and Kafka Broker)

### 2. Start Kafka locally
Using standard Kafka scripts (Linux/Mac):
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```

Or using Docker:
```bash
docker run -p 9092:9092 -e KAFKA_ZOOKEEPER_CONNECT=localhost:2181 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 confluentinc/cp-kafka:latest
```

### 3. Build the Project
```bash
cd KafkaStreamsStreamingPatterns
mvn clean compile
```

## 🏃 Running the Examples
Each file contains a `main` method. You can run them directly from your IDE (IntelliJ, Eclipse, VSCode) or via maven exec. 

For example, to run the Basic Operations example:
```bash
mvn exec:java -Dexec.mainClass="com.learning.kafkastreaming.basics.BasicOperations"
```

## 🛠️ Built With
- **Kafka Streams** (2.6.0)
- **Java** 11
- **Jackson** (JSON Serialization)
- **Redis (Jedis)** & **MariaDB** (for legacy chapters) 
