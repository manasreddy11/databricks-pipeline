# Databricks notebook source
import dlt 
from pyspark.sql.functions import *
from pyspark.sql.types import * 

# COMMAND ----------

# Your existing constraints
constraints = {
    'valid_id': 'CRASH_RECORD_ID IS NOT NULL',
    'valid_crash_date': 'CRASH_DATE IS NOT NULL',
    'valid_street_name': 'STREET_NAME IS NOT NULL',
    'valid_street_direction': 'STREET_DIRECTION IS NOT NULL'
}

# Function for the silver table
@dlt.table(
    comment='cleaning and expectation handling'
)
@dlt.expect_all_or_drop(constraints)
def silver():
    silver_df = spark.table('bronze').drop('CRASH_DATE_EST_I', 'LANE_CNT', 'REPORT_TYPE', 'INTERSECTION_RELATED_I', 'NOT_RIGHT_OF_WAY_I', 'HIT_AND_RUN_I', 'PHOTOS_TAKEN_I', 'STATEMENTS_TAKEN_I', 'DOORING_I', 'WORK_ZONE_I', 'WORK_ZONE_TYPE', 'WORKERS_PRESENT_I', 'MOST_SEVERE_INJURY', 'INJURIES_TOTAL', 'INJURIES_FATAL',
          'INJURIES_INCAPACITATING', 'INJURIES_NON_INCAPACITATING',
          'INJURIES_REPORTED_NOT_EVIDENT', 'INJURIES_NO_INDICATION',
          'INJURIES_UNKNOWN', 'LATITUDE', 'LONGITUDE', 'LOCATION')
    silver_df.write.mode('overwrite').saveAsTable('silver')
    return silver_df

# Function for the quarantine table
@dlt.table(
    name="quarantine_data",
    comment="Table for quarantined records"
)
def quarantine_table():
    quarantine_df = spark.table('bronze').filter("NOT ({})".format(" AND ".join(constraints.values())))
    quarantine_df.write.mode('overwrite').saveAsTable('quarantinedData')
    return quarantine_df
