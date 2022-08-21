#!/usr/bin/env python3
__author__ = 'brad'
"""
Class to test PySpark client
"""

import sys, os
myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + '/../')
import pytest
from pyspark.sql import SparkSession
import my_app as ma

# important properties to set to enable Hive testing
spark = SparkSession.builder.appName('com.dadoverflow.mypysparkclient.tests') \
    .master('local[*]') \
    .enableHiveSupport() \
    .config('spark.sql.legacy.createHiveTableByDefault', 'false') \
    .getOrCreate()
    
# name of test database and test table to use in my unit tests
test_db, test_table = 'some_db', 'my_test_table'


def get_script_text(script_file):
    """
    Helper function to pull in text of CREATE TABLE scripts for testing
    """
    script_text = ''
    fn = '{0}/../hive_scripts/{1}'.format(myPath, script_file)

    with open(fn, 'r') as f:
        script_text = f.readlines()

    return ' '.join(script_text)


@pytest.fixture
def setup_hive(scope='module'):
    #setup hive database for testing
    spark.sql(f'CREATE DATABASE {test_db}')
    spark.sql(get_script_text('create_table_my_test_table.sql'))
    yield

    #tear down database when testing is finished
    spark.sql(f'DROP TABLE {test_db}.{test_table}')
    spark.sql(f'DROP DATABASE {test_db}')


def test_write_some_data_to_hive(setup_hive):
    test_data = [('Ward', 'Cleaver', 35), ('June', 'Cleaver', 34), ('Wally', 'Cleaver', 10), ('Theodore', 'Cleaver', 7)]

    test_df = ma.build_dataframe(test_data)
    ma.save_dataframe(test_df, test_db, test_table)

    table_data = spark.sql(f'SELECT * FROM {test_db}.{test_table}')
    assert table_data.count() == len(test_data), 'Test failed: expected {0} records written to the test table'.format(len(test_data))

