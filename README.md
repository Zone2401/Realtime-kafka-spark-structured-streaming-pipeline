# Real-time User Data Pipeline with Airflow, Kafka, Spark Structured Streaming, and Cassandra

A scalable real-time data pipeline that ingests API data via Apache Airflow, streams events with Kafka, performs real-time processing using Spark Structured Streaming, and stores processed data in Cassandra.

## Technologies Used

- **Apache Airflow**: Data orchestration and workflow management
- **Apache Kafka**: Real-time data streaming and message brokering
- **Apache Spark**: Distributed stream processing
- **Apache Cassandra**: NoSQL database for scalable storage
- **Docker**: Containerization and orchestration

## Architecture

<img width="1185" height="723" alt="{6C1893F3-8F6E-453E-B0A2-1EA57286E888}" src="https://github.com/user-attachments/assets/9b086a20-d664-4c30-959e-8b73fa2d5229" />





## Setup Instructions

### 1. Clone Repository

```bash
git clone [https://github.com/Zone2401/Realtime-kafka-spark-structured-streaming-pipeline]
cd streaming_prj
```

### 2. Directory Structure

```
streaming_prj/
├── dags/
│   ├── kafka_stream.py          # Kafka streaming DAG
│   └── streaming_to_kafka.py    # Airflow DAG for data ingestion
├── jobs/
│   └── spark_streaming.py       # Spark Structured Streaming job
├── script/
│   └── entrypoint.sh            # Airflow initialization script
├── docker-compose.yml           # Docker services configuration
└── requirements.txt             # Python dependencies
```

## How to Run

### 1. Start Docker Containers

```bash
docker-compose up -d
```

Wait for all services to be healthy (approximately 2-3 minutes).

### 2. Initialize Airflow

1. Access Airflow UI: **http://localhost:8080**
2. Enable the `push_data_to_broker` DAG
3. Trigger the DAG manually or wait for scheduled execution

<img width="1854" height="403" alt="{D5282AFE-E5D2-4A1E-BA17-BEED13CAFB20}" src="https://github.com/user-attachments/assets/65e07c8a-3cdb-470f-8935-fde1f87e61b7" />


### 3. Monitor Kafka via Control Center

Access Confluent Control Center: **http://localhost:9021**

Check ZooKeeper, Kafka broker, and topics status.

<img width="740" height="915" alt="{AB25D569-C3BE-45CC-9157-FC6847224BFA}" src="https://github.com/user-attachments/assets/73aff6db-dd2c-484d-8529-751d7ddcaca2" />


### 4. Verify Data in Kafka Topic

Navigate to **Topics** → `users_profile` to view messages.

<img width="1896" height="913" alt="{E86967AE-C346-471E-89C8-D92A25C95D20}" src="https://github.com/user-attachments/assets/01667f91-d3ab-4441-b204-9fc068a73875" />




### 5. Run Spark Structured Streaming


```bash
docker exec --user root spark-master bash -c "spark-submit --master spark://spark-master:7077 --conf 'spark.driver.extraJavaOptions=-Duser.home=/tmp' --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 /opt/bitnami/spark/jobs/spark_streaming.py" 
```

<img width="868" height="196" alt="{D8F3FC0D-729B-4FC8-83D2-D9C167637859}" src="https://github.com/user-attachments/assets/b9f78d7b-efe0-476a-864f-75e9ee7b239b" />


### 6. Verify Data in Cassandra
If Cassandra not found
```bash
docker exec --user root  spark-master pip install cassandra-driver  
```
After that
```bash
docker exec -it cassandra cqlsh -u cassandra -p cassandra localhost 9042
USE spark_streams;
SELECT * FROM users_profile LIMIT 10;
```

<img width="1750" height="544" alt="{A693368A-5FD6-4A66-922A-6DE6E7EA8575}" src="https://github.com/user-attachments/assets/f27d5326-0633-4e26-9294-722dddb98e03" />


## Key Components

### Docker Services

| Service | Port | Description |
|---------|------|-------------|
| **Airflow Webserver** | 8080 | Workflow orchestration UI |
| **Kafka Broker** | 9092 | Message streaming platform |
| **ZooKeeper** | 2181 | Kafka cluster coordination |
| **Control Center** | 9021 | Kafka monitoring |
| **Spark Master** | 7077, 9090 | Spark cluster management |
| **Cassandra** | 9042 | NoSQL data storage |

### Airflow DAG

Fetches user data from Random User API every 2 seconds for 5 minutes and streams to Kafka topic `users_profile`.

### Spark Streaming

Consumes from Kafka, parses JSON, and writes to Cassandra table `spark_streams.users_profile`.

### Cassandra Schema

```sql
CREATE TABLE spark_streams.users_profile (
    id TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    gender TEXT,
    address TEXT,
    email TEXT,
    username TEXT,
    dob TEXT,
    registered_date TEXT,
    phone TEXT,
    picture TEXT
);
```

## Useful Commands

### Kafka

```bash
# List topics
docker exec -it broker kafka-topics --list --bootstrap-server localhost:9092

# View messages
docker exec -it broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic users_profile \
  --from-beginning
```

### Cassandra

```bash
# Check status
docker exec -it cassandra nodetool status

# Count records
docker exec -it cassandra cqlsh -e "SELECT COUNT(*) FROM spark_streams.users_profile;"
```

### Spark

Access Spark Master UI: **http://localhost:9090**

## Troubleshooting

### Reset System

```bash
docker-compose down -v
docker-compose up -d
```

### Common Issues

- **Kafka timeout**: Check broker health with `docker logs broker`
- **Cassandra connection**: Wait 1-2 minutes for initialization
- **Memory errors**: Increase Docker memory to 8GB+

## Useful Links

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Apache Cassandra Documentation](https://cassandra.apache.org/doc/latest/)
- [Random User API](https://randomuser.me/)

---

