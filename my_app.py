#!/usr/bin/env python3
__author__ = 'brad'
"""
My PySpark client
"""

import sys
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructField, StructType, StringType
from pyspark.sql.functions import current_timestamp, input_file_name


spark = SparkSession.builder.appName('com.dadoverflow.mypysparkclient').getOrCreate()
log4jLogger = spark._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)


def build_dataframe(data):
    """Simple function to build a dataframe"""
    df = spark.createDataFrame(data, ['fname', 'lname', 'age'])
    return df


def save_dataframe(df, dbname, table_name):
    """Simple function to save a dataframe to Hive"""
    df = df.withColumn('insert_timestamp', current_timestamp())
    df.write.mode('append').insertInto(f'{dbname}.{table_name}')


def main(argv):
    LOGGER.info('Starting application')
    some_data = [('Homer', 'Simpson', 35), ('Marge', 'Simpson', 32)]
    df = build_dataframe(some_data)
    save_dataframe(df, 'some_db', 'some_table')
    LOGGER.info('Completing application')


if __name__ == "__main__":
    main(sys.argv[1:])
