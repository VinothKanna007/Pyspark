from pyspark import SparkConf
from pyspark.sql import SparkSession

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# 1. spark Connection
# 2. read_csv using df
# 3. simple aggregation
# 4. there is no dataset[<object>] in python

my_conf = SparkConf()
my_conf.set("spark.app.name", "w11_Spark_Dataframes_1_2_3_4")
my_conf.set("spark.master", "local[*]")
#C:\Users\Vinoth\Desktop\Old_Files\shared1\Week11_Spark
spark = SparkSession.builder.config(conf = my_conf).getOrCreate()

orders_df = (spark.read.
             option("header", "True").
             option("inferSchema", "True").
             csv("C:/Users/Vinoth/Desktop/Old_Files/shared1/Week11_Spark/orders.csv"))

agg_df = (orders_df.
          repartition(4).
          where("order_customer_id > 10000").
          select("order_customer_id", "order_id").
          groupby("order_customer_id").
          count())

agg_df.show()

agg_df.printSchema()

spark.stop()
