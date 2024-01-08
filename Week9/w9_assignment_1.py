from pyspark import SparkContext
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

sc = SparkContext("local[*]", "w9_assignment_1")
sc.setLogLevel("ERROR")


def age(input):
    fields = input.split(",")
    age = int(fields[1])
    name = fields[0]
    city = fields[2]
    if(age > 18):
        return (name, age, city, 'Y')
    else:
        return (name, age, city, 'N')


age_data = sc.textFile("/Users/Vinoth/Desktop/Old_Files/shared1/Week9_Spark/Assignment/dataset1")

results = age_data.map(age).collect()

for result in list(results):
    print(result)