import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def create_spark_connection():
    try:
        spark_conn = SparkSession.builder \
            .appName('SparkDataStreamming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .config("spark.cassandra.connection.port","9042") \
            .getOrCreate()
        spark_conn.sparkContext.setLogLevel("ERROR")
        print("Spark connection created successfully!")
    except Exception as e:
        print(f"Couldn't create the spark session due to exception {e}")
        spark_conn = None 
    return spark_conn

def initial_df_from_kafka(spark_conn):
    print("Connecting to Kafka...")
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'users_profile') \
            .option("startingOffsets", "earliest") \
            .load()
        print("Kafka dataframe created successfully")
    except Exception as e:
        print(f"Kafka dataframe could not be created because: {e}")
        spark_df = None
    return spark_df

def final_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("dob", StringType(), False),             
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    return sel

def create_cassandra_connection():
    print("Creating Cassandra connection...")
    try:
        cluster = Cluster(['cassandra'])
        session = cluster.connect()
        return session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to {e}")
        return None
    
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("Keyspace created successfully!")

def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.users_profile (
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
        picture TEXT);
    """)
    print("Table created successfully!")

def insert_data_to_cassandra(final_df):
    print('Inserting...')
    streaming_query = final_df.writeStream.format("org.apache.spark.sql.cassandra") \
                                            .option('checkpointLocation', '/tmp/checkpoint') \
                                            .outputMode("append") \
                                            .options(table = 'users_profile', keyspace = 'spark_streams') \
                                            .start()
    streaming_query.awaitTermination()

if __name__ == "__main__":
    spark_conn = create_spark_connection()
    if spark_conn is not None:
        df = initial_df_from_kafka(spark_conn)
        final_df = final_df_from_kafka(df)
        session = create_cassandra_connection()
        if session is not None:
            create_keyspace(session)
            create_table(session)
            print("Streaming is being started...")
            insert_data_to_cassandra(final_df)