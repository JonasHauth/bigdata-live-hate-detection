import timeit
from statistics import mean
import numpy as np

import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, regexp_replace, lower
import os




def prepare_test_environment():

    MAX_MEMORY = "6g"
    spark = SparkSession.builder \
                        .appName('multi_class_text_classifiter')\
                        .master("local[8]") \
                        .config("spark.executor.memory", MAX_MEMORY) \
                        .config("spark.driver.memory", MAX_MEMORY) \
                        .getOrCreate()

    print("Apache Spark version: ", spark.version)

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    df = spark.read.json("C://data/twitter-stream-json-small/*.json")
    print(f'Test Json: {df.count()}')

    df = spark.read.parquet("C://data/twitter-stream-parquet-small/")
    print(f'Test Parquet: {df.count()}')

    print("Test environment prepared.")

    return spark

def read_json_small(spark):
    df = spark.read.json("C://data/twitter-stream-json-small/*.json")
    print(df.count())

def read_json_medium(spark):
    df = spark.read.json("C://data/twitter-stream-json-medium/*.json")
    print(df.count())

def read_json_complete(spark):
    df = spark.read.json("C://data/twitter-stream-json-complete/*.json")
    print(df.count())


def read_parquet_small(spark):
    df = spark.read.parquet("C://data/twitter-stream-parquet-small/")
    print(df.count())

def read_parquet_medium(spark):
    df = spark.read.parquet("C://data/twitter-stream-parquet-medium/")
    print(df.count())

def read_parquet_complete(spark):
    df = spark.read.parquet("C://data/twitter-stream-parquet-complete/")
    print(df.count())


if __name__ == "__main__":

    spark = prepare_test_environment()

    print("Time Read json small ...")
    read_json_small = timeit.repeat("read_json_small(spark)", repeat=1, number=1, globals=locals())
    print(f"Time Json small: {mean(read_json_small)} s, standard deviation {np.std(read_json_small)}")



    print("Time Read json medium ...")
    read_json_medium = timeit.repeat("read_json_medium(spark)", repeat=1, number=1, globals=locals())
    print(f"Time Json medium: {mean(read_json_medium)} s, standard deviation {np.std(read_json_medium)}")



    print("Time Read json complete ...")
    read_json_complete = timeit.repeat("read_json_complete(spark)", repeat=1, number=1, globals=locals())
    print(f"Time Json complete: {mean(read_json_complete)} s, standard deviation {np.std(read_json_complete)}")



    print("Time Read parquet small ...")
    read_parquet_small = timeit.repeat("read_parquet_small(spark)", repeat=1, number=1, globals=locals())
    print(f"Time parquet small: {mean(read_parquet_small)} s, standard deviation {np.std(read_parquet_small)}")



    print("Time Read parquet medium ...")
    read_parquet_medium = timeit.repeat("read_parquet_medium(spark)", repeat=1, number=1, globals=locals())
    print(f"Time parquet medium: {mean(read_parquet_medium)} s, standard deviation {np.std(read_parquet_medium)}")



    print("Time Read parquet complete ...")
    read_parquet_complete = timeit.repeat("read_parquet_complete(spark)", repeat=1, number=1, globals=locals())
    print(f"Time parquet complete: {mean(read_parquet_complete)} s, standard deviation {np.std(read_parquet_complete)}")