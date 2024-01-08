from pyspark import SparkContext
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# word count in pyspark

sc = SparkContext("local[*]", "w9 Spark practical 1")

input = sc.textFile("/Users/Vinoth/Desktop/Old_Files/shared1/Week9_Spark/search.txt")

words = input.flatMap(lambda line: line.split(" "))

word_map = words.map(lambda word: (word, 1))

final = word_map.reduceByKey(lambda x, y: x + y)

results  = final.collect()

for result in results:
     print(result)
