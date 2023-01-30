import timeit
from statistics import mean
import numpy as np

import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, regexp_replace, lower
import os

from pyspark.ml import Pipeline
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer



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

    spark_df = spark.read.parquet('./data/parquet_data')
    spark_df = spark_df.withColumnRenamed("tweet_text","text")
    spark_df.count()



    spark_df_sample_small = spark_df.sample(fraction=0.1) 
    print(f'Pipeline small: {spark_df_sample_small.count()}')

    spark_df_sample_medium = spark_df.sample(fraction=0.5) 
    print(f'Pipeline medium: {spark_df_sample_medium.count()}')

    spark_df_sample_complete = spark_df
    print(f'Pipeline complete: {spark_df_sample_complete.count()}')


    hate_train_df, hate_test_df = spark_df_sample_small.randomSplit([0.8, 0.2])

    # 'We hate religion' > 'We' 'hate' 'religion'
    regexTokenizer = RegexTokenizer(inputCol="text", outputCol="tokens", pattern="\\W")
    # Remove stop words
    stopwordsRemover = StopWordsRemover(inputCol="tokens", outputCol="filtered")
    # Term frequency
    countVectors = CountVectorizer(inputCol="filtered", outputCol="features", vocabSize=10000, minDF=5)
    # Classifier
    classifier = NaiveBayes(smoothing=1, labelCol="majority_label", featuresCol="features")

    hate_train_df, hate_test_df = spark_df_sample_small.randomSplit([0.8, 0.2])
    inference_pipeline = Pipeline(stages=[regexTokenizer, stopwordsRemover, countVectors, classifier])
    trained_inference_pipeline = inference_pipeline.fit(hate_train_df)
    predictions = trained_inference_pipeline.transform(hate_test_df)

    print("Test environment prepared.")

    return spark, regexTokenizer, stopwordsRemover, countVectors, classifier, spark_df_sample_small, spark_df_sample_medium, spark_df_sample_complete

def pipeline(spark, regexTokenizer, stopwordsRemover, countVectors, classifier, spark_df_sample):
    hate_train_df, hate_test_df = spark_df_sample.randomSplit([0.8, 0.2])
    inference_pipeline = Pipeline(stages=[regexTokenizer, stopwordsRemover, countVectors, classifier])
    trained_inference_pipeline = inference_pipeline.fit(hate_train_df)
    predictions = trained_inference_pipeline.transform(hate_test_df)


if __name__ == "__main__":

    spark, regexTokenizer, stopwordsRemover, countVectors, classifier, spark_df_sample_small, spark_df_sample_medium, spark_df_sample_complete = prepare_test_environment()

    print("Time training pipeline small ...")
    pipeline_small = timeit.repeat("pipeline(spark, regexTokenizer, stopwordsRemover, countVectors, classifier, spark_df_sample_small)", repeat=5, number=1, globals=locals())
    print(f"{pipeline_small}")
    print(f"Time Json small: {mean(pipeline_small)} s, standard deviation {np.std(pipeline_small)}")

    print("Time training pipeline medium ...")
    pipeline_medium = timeit.repeat("pipeline(spark, regexTokenizer, stopwordsRemover, countVectors, classifier, spark_df_sample_medium)", repeat=5, number=1, globals=locals())
    print(f"{pipeline_medium}")
    print(f"Time Json medium: {mean(pipeline_medium)} s, standard deviation {np.std(pipeline_medium)}")

    print("Time training pipeline complete ...")
    pipeline_complete = timeit.repeat("pipeline(spark, regexTokenizer, stopwordsRemover, countVectors, classifier, spark_df_sample_complete)", repeat=5, number=1, globals=locals())
    print(f"{pipeline_complete}")
    print(f"Time Json complete: {mean(pipeline_complete)} s, standard deviation {np.std(pipeline_complete)}")
    	