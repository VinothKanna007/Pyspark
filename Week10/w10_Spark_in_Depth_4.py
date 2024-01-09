from pyspark import SparkContext
import os
import sys


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Groupby Vs reduceByKey
sc = SparkContext("local[*]", "w10_Spark_in_Depth_4")
sc.setLogLevel("ERROR")


input = sc.textFile("/Users/Vinoth/Desktop/Old_Files/shared1/Week10_Spark/bigLog.txt")

input_mapped = input.map(lambda line: (line.split(":")[0],1))

results = input_mapped.groupByKey().map(lambda x: (x[0],len(x[1]))).collect()
# results = input_mapped.reduceByKey(lambda x,y: (x+y)).collect()
for result in results:
    print(result)

sys.stdin.readline()