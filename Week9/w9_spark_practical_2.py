from pyspark import SparkContext
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Additional items added:
#   logger
#   holding the process using stdin
#   DAG

if __name__ == "__main__":

    sc = SparkContext("local[*]", "Spark practical 2")
    sc.setLogLevel("ERROR")

    input = sc.textFile("/Users/Vinoth/Desktop/Old_Files/shared1/Week9_Spark/search.txt")

    words = input.flatMap(lambda line: line.split(" "))

    word_map = words.map(lambda word: (word, 1))

    final = word_map.reduceByKey(lambda x, y: x + y)

    results  = final.collect()

    for result in results:
        print(result)
else:
    print("Not executed directly")

sys.stdin.readline()