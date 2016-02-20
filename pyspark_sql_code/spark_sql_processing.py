# simple way to launch this is '/usr/bin/spark-submit ./spark_sql_processing.py'

# import critical modules
from __future__ import print_function # support python 2.7 & 3
from pyspark import SparkContext
import datetime

# define params
s3_target_bucket_name = 'mattsona-public'

sc = SparkContext() # create a spark context
