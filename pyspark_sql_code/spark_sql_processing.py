# simple way to launch this is '/usr/bin/spark-submit ./spark_sql_processing.py'

# import critical modules
from __future__ import print_function # support python 2.7 & 3
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.types import *
import datetime

# define params
s3_target_bucket_name = 'mattsona-public'

sc = SparkContext() # create a spark context
sql_context = HiveContext(sc) # create a Hive context

# create an RDD from the source file first
# load data file from s3
print('Loading file from S3...')
source_file = sc.textFile('s3://mattsona-public/spark_demo_data/shirt_orders_header.csv')
