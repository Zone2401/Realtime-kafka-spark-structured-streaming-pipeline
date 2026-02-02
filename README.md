# Real-time User Data Pipeline with Airflow, Kafka, Spark Structured Streaming, and Cassandra

A scalable real-time data pipeline that ingests API data via Apache Airflow, streams events with Kafka, performs real-time processing using Spark Structured Streaming, and stores processed data in Cassandra.

## Technologies Used

- **Apache Airflow**: Data orchestration and workflow management
- **Apache Kafka**: Real-time data streaming and message brokering
- **Apache Spark**: Distributed stream processing
- **Apache Cassandra**: NoSQL database for scalable storage
- **Docker**: Containerization and orchestration

## Architecture


<img width="1192" height="733" alt="{6FD74F86-EB29-470A-B912-A23D349D0EEC}" src="https://github.com/user-attachments/assets/4cc353b3-f9c4-45b9-9dc4-ced876c1852a" />




## Setup Instructions

### 1. Clone Repository

```bash
git clone [my-repo-url]
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

<img width="1920" height="874" alt="{53F02FDB-09B7-45BC-AE0F-39D00DFA4568}" src="https://github.com/user-attachments/assets/d04ef4d1-b82c-40fb-99e1-9a057ca12a14" />


### 3. Monitor Kafka via Control Center

Access Confluent Control Center: **http://localhost:9021**

Check ZooKeeper, Kafka broker, and topics status.

<img width="735" height="880" alt="{DC9A233A-E5DA-47D2-BA5E-3AA08BDAC577}" src="https://github.com/user-attachments/assets/a538b908-52fe-4400-ad41-73bea02edce0" />


### 4. Verify Data in Kafka Topic

Navigate to **Topics** → `users_profile` to view messages.

<img width="1903" height="878" alt="{8D39A2B3-16D4-4C13-BFA6-45E2B5581312}" src="https://github.com/user-attachments/assets/d341560d-a5da-416f-9c4e-df2a0a5aca20" />



### 5. Run Spark Structured Streaming


```bash
docker exec --user root spark-master bash -c "spark-submit --master spark://spark-master:7077 --conf 'spark.driver.extraJavaOptions=-Duser.home=/tmp' --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 /opt/bitnami/spark/jobs/spark_streaming.py" 
```

<img width="1316" height="280" alt="{CD688771-6ACE-4172-976B-6E251A44A695}" src="https://github.com/user-attachments/assets/606af4b0-9220-40d8-877c-300f03cf8950" />

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

<img width="1397" height="827" alt="{2598709D-2AAD-44A8-9156-4B8C3516E63E}" src="https://github.com/user-attachments/assets/6da91890-2fad-4491-837f-1c7d316b8c55" />

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

