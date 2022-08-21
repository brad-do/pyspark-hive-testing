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

spark = SparkSession.builder.appName('com.dadoverflow.mypysparkclient.tests').getOrCreate()

def test_build_dataframe():
    test_data = [('Al', 'Bundy', 40), ('Peg', 'Bundy', 39), ('Kelly', 'Bundy', 16), ('Bud', 'Bundy', 14)]
    test_df = ma.build_dataframe(test_data)

    assert len(test_df.columns) == 3, 'Test failed: expected 3 columns in dataframe'
    assert test_df.count() == 4, 'Test failed: expected 4 rows in dataframe'
