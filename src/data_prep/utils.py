from pyspark.sql import SparkSession
import json

def create_pyspark_session():
    spark = SparkSession \
        .builder \
        .master("local") \
        .appName("Python Spark entel datathon 2022") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark 

def load_dict():
    f = open('src/variable_entrada.json')
    data = json.load(f)
    return data

def load_data(df_path, spark):
    return spark.read.option("header", True).option("inferSchema", True).csv(df_path)

