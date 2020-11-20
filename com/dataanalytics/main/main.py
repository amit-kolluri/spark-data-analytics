import os
import tempfile

import com.dataanalytics.utility.constants as const
import com.dataanalytics.utility.messageLogger as ml
import com.dataanalytics.utility.processFileOps as dataO

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType


def main():
    spark = SparkSession \
        .builder \
        .master("local[1]") \
        .appName("Spark Data Operations") \
        .config('spark.driver.extraClassPath', r'C:\Users\amitk\Documents\softwares\postgresql-42.2.18.jar') \
        .config("spark.sql.catalogImplementation", "hive") \
        .getOrCreate()
    df, msg = testSparkInstallation(spark, const)

    # Start Logger
    messageLogger = ml.MessageLogger(const.getProjectName(__file__), "Logger Started")
    messageLogger.logInfo("Logger Started")
    messageLogger.logInfo(msg)

    # write temp df into csv, parquet and json
    # test_spark_installation_output = "test_spark_installation_output"
    # processData = dataO.ProcessFileOps(spark)
    # processData.processCSVoutput(df, test_spark_installation_output)
    # processData.processParquetOutput(df, test_spark_installation_output, 2, 'gender')
    # processData.processJsonOutput(df, test_spark_installation_output)

    # File operations with Data Frames

    # import com.dataanalytics.services.project1 as p1
    # project1 = p1.DataFrameOperations(spark)
    # df = project1.readFromCsv()
    # project1.dataFrameExamples(df)
    # project1.writeToDB(df)



    #Database operations

    import com.dataanalytics.services.project2 as p2
    proj2 = p2.DatabaseOps(spark)
    proj2.write_to_db(df)
    # proj2.read_from_db()



     # Assignment: SQL TempViews

    # import com.dataanalytics.services.project3 as p3
    # project3 = p3.ReadCsv(spark, "messageLogger")
    # project3.readFromCsv()

    ## Test Printing
    # print(spark.sparkContext.appName)
    # configurations = spark.sparkContext.getConf().getAll()
    # for conf in configurations:
    #     print(conf)

    const.updateJobRunId()
    # const.remove_all_folders(const.processedFile)


def testSparkInstallation(spark, const):
    df = const.createEmptyDF(spark)
    try:
        data = [('James', '', 'Smith', '1991-04-01', 'M', 3000),
                ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
                ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
                ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
                ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)]

        columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
        df = spark.createDataFrame(data=data, schema=columns)
        msg = "Spark Installed Succesfully"
    except Exception as e:
        msg = str(e.__class__)
    return df, msg


if __name__ == "__main__":
    main()
