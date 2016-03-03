# simple way to launch this is '/usr/bin/spark-submit ./spark_sql_processing.py'

# import critical modules
from __future__ import print_function # support python 2.7 & 3
from pyspark import SparkContext
from pyspark.sql import HiveContext, Row
from pyspark.sql.types import *
import datetime

# define params
s3_target_bucket_name = 'mattsona-spark-demo'

sc = SparkContext() # create a spark context
sql_context = HiveContext(sc) # create a Hive context

# create an RDD from the source file first
# load data file from s3
print('Loading file from S3...')
source_file = sc.textFile('s3://mattsona-public/spark_demo_data/shirt_orders.csv')
time_stamp = datetime.datetime.isoformat(datetime.datetime.now()).replace(':','_') # create a datestamp for the DataFrame to be saved

# split each line on ',' as this is a csv
lines = source_file.map(lambda x: x.split(','))
lines_inferred_types_rows = lines.map(lambda x: Row(full_name = x[0], shirt_size  = x[1], us_state = x[2], shirt_quantity = int(x[3])))

# turn it in to a DataFrame
lines_schema = sql_context.createDataFrame(lines_inferred_types_rows)
lines_schema.registerTempTable("t_shirt_orders")

# execute SQL statement over DataFrame for quantity >= 100
orders_over_100 = sql_context.sql("SELECT * FROM t_shirt_orders WHERE shirt_quantity >= 100")
# save it to JSON to retain schema (could also use ORC, Parquet, etc)
orders_over_100.write.json('s3://' + s3_target_bucket_name + '/spark_sql_processing/' + time_stamp + '/json')
