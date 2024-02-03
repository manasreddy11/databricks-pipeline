# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

path = 's3://mydatabucket99/Traffic_Crashes_-_Crashes 2.csv'

@dlt.table (
    comment = 'ingesting the data layer = Bronze'
)

def ingest_data():
    df = spark.read.option('header', True).option('inferSchema', True).csv(path)
    bronze_table = df.write.mode('overwrite').saveAsTable('bronze')
    return bronze_table
  
