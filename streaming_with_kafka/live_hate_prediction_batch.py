import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import time
from pyspark.ml import PipelineModel
import os


KAFKA_TOPIC_NAME_CONS = "test-topic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:29092'




if __name__ == "__main__":
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka and Message Format as JSON") \
        .master("local[4, 2]") \
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.3.1") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from test-topic
    tweets_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
        .option("startingOffsets", "latest") \
        .load()


    def clean_text(c):
        c = lower(c)
        c = regexp_replace(c, "(https?\://)\S+", "") # Remove links
        c = regexp_replace(c, "(\\n)|\n|\r|\t", "") # Remove CR, tab, and LR
        c = regexp_replace(c, "(?:(?:[0-9]{2}[:\/,]){2}[0-9]{2,4})", "") # Remove dates
        c = regexp_replace(c, "@([A-Za-z0-9_]+)", "") # Remove usernames
        c = regexp_replace(c, "[0-9]", "") # Remove numbers
        c = regexp_replace(c, "\:|\/|\#|\.|\?|\!|\&|\"|\,", "") # Remove symbols
        return c

    trained_pipeline = PipelineModel.load("./models/mllib_model_nb")


    # Define a schema for the orders data
    # order_id,order_product_name,order_card_type,order_amount,order_datetime,order_country_name,order_city_name,order_ecommerce_website_name
    tweets_schema = StructType() \
        .add("order_id", StringType()) \
        .add("text", StringType()) \
        .add("tweet_datetime", StringType())

    print("Printing Schema of tweets_df: ")
    tweets_df.printSchema()
    tweets_df1 = tweets_df.selectExpr("CAST(value AS STRING)", "timestamp")

    tweet_df2 = tweets_df1\
        .select(from_json(col("value"), tweets_schema).alias("data"), "timestamp").select("data.text", "timestamp")

    # Clean Text Columns
    tweet_df3 = tweet_df2.withColumn("text", clean_text(col("text")))

    # Predict on Batch
    final_df = trained_pipeline.transform(tweet_df3).select('text','prediction', 'timestamp')

    # For Testing
    batchHateInference = final_df.writeStream \
        .format("console") \
        .start()

    # Output to Kafka
    # liveHateInference = final_df.select(to_json(struct("text", "prediction", "timestamp")).alias("value")) \
    #     .writeStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
    #     .option("topic", "topic1") \
    #     .option("checkpointLocation", "/data/checkpoints") \
    #     .trigger(continuous="1 second") \
    #     .start()

    #.trigger(processingTime='2 seconds') \

    batchHateInference.awaitTermination()

    print("Stream Data Processing Application Completed.")

   
