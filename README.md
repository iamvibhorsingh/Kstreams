# Comprehensive Kafka Streams Guide (Java 17 / Kafka 3.8+)

A complete, production-grade collection of **Apache Kafka Streams** design patterns and architectural recipes. Modernized for **Java 17+ (compatible with Java 21)** and **Kafka Streams 3.8.0**, covering everything from fundamental stateless transformations to advanced topics like Exactly-Once Semantics (EOS V2), low-level Processor APIs (KIP-405), Foreign Key Table Joins (KIP-213), Window Suppression, Versioned State Stores (KIP-889), and RocksDB off-heap performance tuning.

---

## Architecture & Tech Stack

- **Java**: 17 LTS / 21 LTS
- **Apache Kafka Streams**: `3.8.0` (KRaft Mode, EOS V2)
- **Logging**: SLF4J 2.x + Logback Classic (CVE-free)
- **Testing**: JUnit 5 + AssertJ + `TopologyTestDriver`
- **Serialization**: Generic Jackson `JsonSerde<T>`
- **Infrastructure**: Single-node KRaft Kafka Broker + AKHQ Web UI + Redis + MariaDB via Docker Compose

---

## Pattern Catalog

All patterns are organized by package under `src/main/java/com/learning/kafkastreaming/`.

### 1. Fundamentals (`basics` package)
- **[`BasicOperations.java`](file:///src/main/java/com/learning/kafkastreaming/basics/BasicOperations.java)**: Stateless stream transformations (`filter`, `map`, `flatMapValues`, `selectKey`, `peek`, `foreach`).
- **[`BranchAndMerge.java`](file:///src/main/java/com/learning/kafkastreaming/basics/BranchAndMerge.java)**: Fluent stream splitting and predicate routing using `split().branch().defaultBranch()` (KIP-632) and stream merging.

### 2. Stateful Operations & Windowing (`stateful` package)
- **[`CountAndReduce.java`](file:///src/main/java/com/learning/kafkastreaming/stateful/CountAndReduce.java)**: Stateful counting and custom reduction on grouped streams.
- **[`AggregationPatterns.java`](file:///src/main/java/com/learning/kafkastreaming/stateful/AggregationPatterns.java)**: Global unbounded aggregations, Session Windows (inactivity gaps with session mergers), and Hopping Windows.
- **[`SlidingWindowsPattern.java`](file:///src/main/java/com/learning/kafkastreaming/stateful/SlidingWindowsPattern.java)**: Continuous sliding window aggregations (`SlidingWindows.ofTimeDifferenceAndGrace`) for real-time anomaly/fraud detection.
- **[`WindowSuppression.java`](file:///src/main/java/com/learning/kafkastreaming/stateful/WindowSuppression.java)**: Suppressing intermediate windowed updates with `Suppressed.untilWindowCloses(...)` to emit only final aggregated results.
- **[`KTableOperations.java`](file:///src/main/java/com/learning/kafkastreaming/stateful/KTableOperations.java)**: Primary-key upsert streams, filtering tables, and table-to-stream changelogs.

### 3. Joins (`joins` package)
- **[`StreamStreamJoin.java`](file:///src/main/java/com/learning/kafkastreaming/joins/StreamStreamJoin.java)**: Inner, Left, and Outer windowed joins (`JoinWindows.ofTimeDifferenceWithNoGrace`) between two `KStream`s.
- **[`StreamTableJoin.java`](file:///src/main/java/com/learning/kafkastreaming/joins/StreamTableJoin.java)**: Co-partitioned `KStream-KTable` joins and broadcast `KStream-GlobalKTable` lookups.
- **[`TableTableJoin.java`](file:///src/main/java/com/learning/kafkastreaming/joins/TableTableJoin.java)**: Continuous stateful primary-key joins between two `KTable`s.
- **[`ForeignKeyJoin.java`](file:///src/main/java/com/learning/kafkastreaming/joins/ForeignKeyJoin.java)**: Non-primary key foreign joins (KIP-213) (e.g. `Orders` referencing `Customers` by `customerId`).

### 4. Advanced Patterns & Performance (`advanced` package)
- **[`ProcessorApiExample.java`](file:///src/main/java/com/learning/kafkastreaming/advanced/ProcessorApiExample.java)**: Modern typed `Processor<KIn, VIn, KOut, VOut>` API (KIP-405) with `Record<K, V>` and wall-clock time `Punctuator` scheduling.
- **[`HeaderPropagationProcessor.java`](file:///src/main/java/com/learning/kafkastreaming/advanced/HeaderPropagationProcessor.java)**: Inspecting, tracing, and mutating Kafka Record Headers across topology stages.
- **[`VersionedStateStoreExample.java`](file:///src/main/java/com/learning/kafkastreaming/advanced/VersionedStateStoreExample.java)**: Point-in-time temporal queries using Kafka 3.5+ versioned key-value stores (`Stores.persistentVersionedKeyValueStore`).
- **[`RocksDBConfigTuning.java`](file:///src/main/java/com/learning/kafkastreaming/advanced/RocksDBConfigTuning.java)**: Custom `RocksDBConfigSetter` tuning block caches (LRU), write buffers, LZ4 compression, and compaction parallelism.
- **[`ExactlyOnceProcessing.java`](file:///src/main/java/com/learning/kafkastreaming/advanced/ExactlyOnceProcessing.java)**: Configuring Exactly-Once Semantics V2 (`processing.guarantee=exactly_once_v2`).
- **[`ErrorHandling.java`](file:///src/main/java/com/learning/kafkastreaming/advanced/ErrorHandling.java)**: Deserialization exception handlers (`LogAndContinue`), Dead Letter Queue (DLQ) routing, and `StreamsUncaughtExceptionHandler` thread recovery.
- **[`InteractiveQueries.java`](file:///src/main/java/com/learning/kafkastreaming/advanced/InteractiveQueries.java)**: Exposing materialized state stores for direct RPC queries via `ReadOnlyKeyValueStore`.
- **[`CustomPartitioner.java`](file:///src/main/java/com/learning/kafkastreaming/advanced/CustomPartitioner.java)**: Custom `StreamPartitioner` (KIP-699) and custom payload `TimestampExtractor`.

### 5. Automated Unit Testing (`testing` package in `src/test/java`)
- **[`TopologyTestDriverTest.java`](file:///src/test/java/com/learning/kafkastreaming/testing/TopologyTestDriverTest.java)**: Fast, cluster-free topology testing with JUnit 5 and AssertJ.
- **[`AggregationTopologyTest.java`](file:///src/test/java/com/learning/kafkastreaming/testing/AggregationTopologyTest.java)**: Validating windowed aggregations and direct `WindowStore` state verification.
- **[`ForeignKeyJoinTopologyTest.java`](file:///src/test/java/com/learning/kafkastreaming/testing/ForeignKeyJoinTopologyTest.java)**: Verifying foreign key table join updates.
- **[`WindowSuppressionTopologyTest.java`](file:///src/test/java/com/learning/kafkastreaming/testing/WindowSuppressionTopologyTest.java)**: Verifying intermediate record suppression and window close events.

---

### Legacy End-to-End Solutions (Chapters 2–6)
- **Chapter 2 (Analytics):** Windowed aggregations writing 5-second summaries to MariaDB JDBC sink.
- **Chapter 3 (Alerts):** Real-time threshold monitoring and high-volume anomaly detection.
- **Chapter 4 (Leaderboards):** Updating game scores in real-time and pushing to a Redis Sorted Set.
- **Chapter 5 (Predictions):** Stream enrichment using external HTTP/NLP sentiment prediction.
- **Chapter 6 (Views):** Keeping track of topics with maximum views using hopping windows and Redis.

---

## Getting Started

### 1. Start Infrastructure (Docker Compose)
Start Kafka in KRaft mode (no ZooKeeper), AKHQ Web UI, Redis, and MariaDB:
```bash
docker compose up -d
```
* **AKHQ Web Console**: [http://localhost:8080](http://localhost:8080)
* **Kafka Bootstrap**: `localhost:9092`
* **Redis**: `localhost:6379`
* **MariaDB**: `localhost:3306`

### 2. Build and Run Automated Tests
You can compile and run all unit tests without any running Kafka cluster:
```bash
cd KafkaStreamsStreamingPatterns
mvn clean test
```

### 3. Run Standalone Examples
Run any example using Maven:
```bash
mvn exec:java -Dexec.mainClass="com.learning.kafkastreaming.basics.BasicOperations"
mvn exec:java -Dexec.mainClass="com.learning.kafkastreaming.joins.ForeignKeyJoin"
mvn exec:java -Dexec.mainClass="com.learning.kafkastreaming.stateful.WindowSuppression"
```
