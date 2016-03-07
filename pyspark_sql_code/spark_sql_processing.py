# demo of running SQL query in spark against CSVs
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

# create an RDDs from both the tshirt order file & state population file
# load data files from s3
print('Loading files from S3...')
tshirt_order_file_source_file = sc.textFile('s3://mattsona-public/spark_demo_data/shirt_orders.csv')
state_population_source_file = sc.textFile('s3://mattsona-public/spark_demo_data/states_with_population.csv')
time_stamp = datetime.datetime.isoformat(datetime.datetime.now()).replace(':','_') # create a datestamp for the DataFrame to be saved

# split each line on ',' as this is a csv
# then convert each line to a row with schema
tshirt_order_file_lines = tshirt_order_file_source_file.map(
    lambda x: x.split(','))
tshirt_order_file_lines_inferred_types_rows = tshirt_order_file_lines.map(
    lambda x: Row(full_name = x[0], shirt_size  = x[1], us_state = x[2], shirt_quantity = int(x[3])))
state_population_lines = state_population_source_file.map(
    lambda x: x.split(','))
state_population_lines_inferred_types_rows = state_population_lines.map(
    lambda x: Row(us_state = x[0], state_population = x[1]))

# turn RDDs in to DataFrames & register with Hive metastore
tshirt_order_file_lines_data_frame = sql_context.createDataFrame(tshirt_order_file_lines_inferred_types_rows)
tshirt_order_file_lines_data_frame.registerTempTable("t_shirt_orders")
state_population_lines_data_frame = sql_context.createDataFrame(state_population_lines_inferred_types_rows)
state_population_lines_data_frame.registerTempTable("state_populations")

# execute SQL statement over DataFrame for quantity >= 100
# orders_over_100 = sql_context.sql("SELECT * FROM t_shirt_orders WHERE t_shirt_orders.shirt_quantity >= 100") # old way with single table
orders_over_100 = sql_context.sql("SELECT t.full_name, t.shirt_size, t.shirt_quantity, t.us_state, p.state_population FROM t_shirt_orders t JOIN state_populations p ON (t.us_state = p.us_state) WHERE t.shirt_quantity >= 100")
# save it to JSON to retain schema (could also use ORC, Parquet, etc)
orders_over_100.write.json('s3://' + s3_target_bucket_name + '/spark_sql_processing/' + time_stamp + '/json')
