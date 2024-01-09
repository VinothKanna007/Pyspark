from pyspark import SparkContext
import os
import sys


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Groupby Vs reduceByKey
sc = SparkContext("local[*]", "w10_Assignment_1")
sc.setLogLevel("ERROR")

def business_rule(input):
    if input[1] >= 90:
        return (input[0], 10)
    elif input[1] >= 50 and input[1] < 90:
        return (input[0], 4)
    elif input[1] >= 25 and input[1] < 50:
        return (input[0], 2)
    else:
        return (input[0], 0)


chapter_csv = sc.textFile("/Users/Vinoth/Desktop/Old_Files/shared1/Week10_Spark/Assignment/chapters.csv")

chapter_csv2 = chapter_csv.map(lambda x: (int(x.split(",")[0]), int(x.split(",")[1])))

chapter_data = chapter_csv2.map(lambda x: (x[1], 1))

course_cnt = chapter_data.reduceByKey(lambda x,y:(x+y))

view_csv = sc.textFile("/Users/Vinoth/Desktop/Old_Files/shared1/Week10_Spark/Assignment/views*.csv")

view_data = view_csv.map(lambda x: (int(x.split(",")[0]), int(x.split(",")[1]))).distinct().map(lambda x: (x[1],x[0]))



joined_data = chapter_csv2.join(view_data)

drop_chapter_id = joined_data.map(lambda x: x[1]).map(lambda x: ((x[0],x[1]),1))

# course_id, views
reducer = drop_chapter_id.reduceByKey(lambda x,y: (x+y)).map(lambda x: (x[0][0], x[1]))

busines_rules = course_cnt.join(reducer).mapValues(lambda x: x[1]/x[0]*100).map(business_rule).reduceByKey(lambda x,y: (x+y))

title_csv = sc.textFile("/Users/Vinoth/Desktop/Old_Files/shared1/Week10_Spark/Assignment/titles.csv").map(lambda x: (int(x.split(",")[0]), x.split(",")[1]))

final_join = busines_rules.join(title_csv).map(lambda x: (x[1])).sortBy(lambda x: x[0], ascending=False).collect()

for i in final_join:
    print(i)