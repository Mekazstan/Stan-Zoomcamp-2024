# pip install spark pyspark

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def create_spark_connection():
    s_conn = None
    """
    This function is used to create a Spark Connection to several services.
    NOTES
     - The AppName are used defined.
     - The .config contains Jar files [These are connectors of different external services (You can include multiple services e.g. Cassandra + Kafka)]
     - If used in localhost use [.config('spark.cassandra.connetion.host', 'localhost')] else use as in below if on docker.
     
    """
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-canssandra-connector_2.13:3.41",
                    "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connetion.host', 'broker') \
            .getOrCreate()
            
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception: {e}")
        
    return s_conn

def connect_to_kafka(spark_conn):
    """_summary_

    Args:
        spark_conn (_type_): 
    
    Description
        Reads data comming from a Kafka Stream upon subscribing to the Kafka topic
        Starting offset is set to read data in the topic starting from the beginning
        NB: Because server runs on broker running on docker & not localhost. Therefore set bootstrap servers as in below.

    Returns:
        A spark Dataframe 
    """
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:9092') \
            .option('subscribe', 'users_created') \
            .option('stratingOffsets', 'earliest') \
            .load()
        logging.info("Kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"Kafka dataframe could nit be created because: {e}")
    
    return spark_df


def create_selection_df_from_kafka(spark_df):
    schema = StructType(
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("email", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("picture", StringType(), False)
    )
    
    # Selecting the data & converting to the specified schema
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)
    
    return sel
    
if __name__ == "__main__":
    # Creating sparking connection
    spark_conn = create_spark_connection() 
    
    if spark_conn is not None:
        # Connect to Kafka with spark & getting the Dataframe
        spark_df = connect_to_kafka(spark_conn)
        
        # Schematizing the Dataframe
        selection_df = create_selection_df_from_kafka(spark_df)
        
        # Now this Data can be populated into a db e.g. Apache Casandra
        streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
            .option('checkpointLocation', '/t')
            .option('keyspace', 'spark_streams')
            .option('table', 'created_users')
            .start())
        
        streaming_query.awaitTermination()