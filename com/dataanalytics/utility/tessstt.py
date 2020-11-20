# from pyspark.sql import SparkSession
import configparser
import os

import pandas as pd

#
# spark = SparkSession \
#     .builder \
#     .master("local[1]") \
#     .appName("Spark Data Operasstions") \
#     .getOrCreate()
#
#
# data = [('James', '', 'Smith', '1991-04-01', 'M', 3000),
#                 ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
#                 ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
#                 ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
#                 ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)]
#
# columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
# df = spark.createDataFrame(data=data, schema=columns)
# df.show()
# fp = open(r'C:\Users\amitk\Documents\Projects\Python\spark-data-analytics\processed_files\test_spark_installxxation_opsssutput.parquet','w+')
# pandasDF = df.toPandas()
# print(pandasDF.to_csv(index=False))
# #print(pandasDF)
#
#
# # df.write.format("parquet").save(fp)
# fp.close()

# df = pd.DataFrame(data={'col1': [1, 2], 'col2': [3, 4]})
# # df.to_parquet(r'C:\Users\amitk\Documents\Projects\Python\spark-data-analytics\processed_files\yybn.gzip',
# #               compression='gzip')
# # pd.read_parquet(r'C:\Users\amitk\Documents\Projects\Python\spark-data-analytics\processed_files\yybn.gzip',)
# from pyspark.sql import SparkSession
#
from pyspark.sql import SparkSession
import os

spark = SparkSession \
    .builder \
    .master("local[1]") \
    .appName("Spark Data Operations") \
    .config('spark.driver.extraClassPath', r'C:\Users\amitk\Documents\softwares\postgresql-42.2.18.jar')\
    .getOrCreate()

#
# db_properties = {}
# config = configparser.ConfigParser()
# config.read("../db_properties.ini")
# db_prop = config['local']
# db_url = db_prop['url']
# db_properties['user'] = db_prop['username']
# db_properties['password'] = db_prop['password']
# db_properties['url'] = db_url
# db_properties['driver'] = db_prop['driver']
# # db_properties['driver'] = "C:/Users/amitk/Documents/softwares/postgresql-42.2.18.jar"
# for a in db_properties:
#     print(a,db_properties[a])
#
# # df = spark.read.jdbc(url=db_url, table="spark_demo.name_details", properties=db_properties)
# # df.show()
#
# data = [('James', 'Smith'),
#         ('Michael', 'Rosdddddde')]
# columns = ["firstname", "lastname"]
# df1 = spark.createDataFrame(data=data, schema=columns)
# df1.write.jdbc(url=db_url, table='spark_demo.name_details', mode= "append", properties=db_properties)
data = [('James', '', 'Smith', '1991-04-01', 'M', 3000),
        ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
        ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
        ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
        ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)]

columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
df = spark.createDataFrame(data=data, schema=columns)
df.show()
fp = r"C:\Users\amitk\Documents\projects\python\spark-data-analytics\processed_files\abcd"
import shutil

#shutil.rmtree(fp)
#os.mkdir(fp)
df.write.csv(fp, header=True)
#   os.removedirs(fp)



# df.write.csv(r"C:\Users\amitk\Documents\projects\python\spark-data-analytics\resources", header=True)
