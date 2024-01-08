from pyspark import SparkContext
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


# Usecase 1 -  Find Top 10 customers who spent max amount

sc = SparkContext("local[*]", "Spark practical 4")
sc.setLogLevel("ERROR")
input = sc.textFile("/Users/Vinoth/Desktop/Old_Files/shared1/Week9_Spark/customerorders.csv")

rdd2 = input.map(lambda x: (x.split(",")[0], float(x.split(",")[2])))

final = rdd2.reduceByKey(lambda x, y: (x + y)).sortBy(lambda x:x[1], ascending=False)

results = final.collect()

for result in results:
    print(result)
