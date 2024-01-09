from pyspark import SparkContext
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

sc = SparkContext("local[*]", "w10_01_real_time_example")
sc.setLogLevel("ERROR")

input = sc.textFile("/Users/Vinoth/Desktop/Old_Files/shared1/Week10_Spark/bigdatacampaigndata.csv")


lines = input.map(lambda x: (float(x.split(",")[10]), x.split(",")[0]))

flat_map = lines.flatMapValues(lambda x: x.split(" ")).map(lambda x: (x[1].lower(), x[0]))

result = flat_map.reduceByKey(lambda x,y: (x+y)).sortBy(lambda x: (x[1]), ascending = False)

print(result.collect())



