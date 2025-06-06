from pyspark import SparkConf
from pyspark.sql import SparkSession

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Read data using Dataframe API (Standard Way)
# Dealing with Corrupted Data
# About parquet file (Spark's Favourite File)

my_conf = SparkConf()
my_conf.set("spark.app.name", "w11_Spark_Dataframes_7")
my_conf.set("spark.master", "local[*]")
#C:\Users\Vinoth\Desktop\Old_Files\shared1\Week11_Spark
spark = SparkSession.builder.config(conf = my_conf).getOrCreate()

# Standary way of dataframe reader            
orderDf = (spark.read.
                format("csv").
                option("header", "True").
                option("inferSchema", "True").
                option("path", "C:/Users/Vinoth/Desktop/Old_Files/shared1/Week11_Spark/orders.csv").
                load())

orderDf.show(2)


orderDf_2 = (spark.read.
                format("json").
                option("header", "True").
                option("inferSchema", "True").
            #   option("mode","DROPMALFORMED").
            #   option("mode","FAILFAST").
                option("mode","PERMISSIVE").  # Default
                option("path", "C:/Users/Vinoth/Desktop/Old_Files/shared1/Week11_Spark/players.json").
                load())

orderDf_2.show()


 
# Metadata is stored in parquet itself (Self describing schema)        
# So Header, inferschema Not required
orderDf_3 = (spark.read.
                option("path", "C:/Users/Vinoth/Desktop/Old_Files/shared1/Week11_Spark/users.parquet").
                load())
                
orderDf_3.printSchema()
orderDf_3.show(truncate=False)
