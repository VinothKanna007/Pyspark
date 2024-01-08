from pyspark import SparkContext
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Additional items added:
# LowerCase
# countByValue
# sortByKey
# sortBy

sc = SparkContext("local[*]", "Spark practical 3")

input = sc.textFile("/Users/Vinoth/Desktop/Old_Files/shared1/Week9_Spark/search.txt")

words = input.flatMap(lambda line: line.split(" "))

# LowerCase
# countByValue

word_map = words.map(lambda word: (word.lower()))
final = word_map.countByValue()
print(final)

# sortByKey
word_map = words.map(lambda word: (word.lower(), 1))
final = word_map.reduceByKey(lambda x, y: x + y).map(lambda x: (x[1], x[0])).sortByKey(ascending=False).map(lambda x: (x[1], x[0]))
results  = final.collect()

for result in results:
     print(result)

# sortBy
word_map = words.map(lambda word: (word.lower(), 1))
final = word_map.reduceByKey(lambda x, y: x + y).sortBy(lambda x: (x[1]), ascending=False)
results  = final.collect()

for result in results:
     print(result)