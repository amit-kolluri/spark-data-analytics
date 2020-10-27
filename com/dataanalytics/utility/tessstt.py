# import regex
#
# #print(regex.findall(r'(?r)\w+(?=\W+\w*e)', 'John likes to eat mushrooms'))
#
# stringgg = r"C:\Users\Amit\Documents\Projects\Pyspark\DataAnalytics\com\dataanalytics\utility\messageLogger.py"
# # *middle, word_list = stringgg.split("\\")
#
#
# word_list = stringgg.replace("\\", "/")
# # print(regex.findall(r'(?r)\w+', stringgg ))
# print(word_list)
from pyspark.sql import SparkSession

columns = ["language", "users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
rdd = spark.sparkContext.parallelize(data)

print(rdd)
