from pyspark import SparkContext
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


# Usecase 2 - How Many times Movies rated
# Example:
# Stars, How many times rated
# 5, 100
# 4, 200
# 3, 19
# 2, 10
# 1, 2

sc = SparkContext("local[*]", "Spark practical 5")
sc.setLogLevel("ERROR")
movie_data = sc.textFile("/Users/Vinoth/Desktop/Old_Files/shared1/Week9_Spark/moviedata.data")

ratings = movie_data.map(lambda x: (x.split("\t")[2],1))

results = ratings.reduceByKey(lambda x, y: x+y).collect()

for result in results:
    print(result)
