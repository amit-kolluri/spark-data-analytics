import com.dataanalytics.utility.constants as const
import com.dataanalytics.utility.messageLogger as ml
from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


class ReadCsv:

    def __init__(self, msg):
        self.msg = msg

    def readFromCsv(self):
        print("Reading from CSV")
        sc = SparkContext()
        sqlContext = SQLContext(sc)
        spark = SparkSession \
            .builder \
            .appName("how to read csv file") \
            .getOrCreate()
        schema = StructType([])
        df = sqlContext.createDataFrame(sc.emptyRDD(), schema)
        print("First SparkContext:");
        print("APP Name :" + spark.sparkContext.appName);
        print("Master :" + spark.sparkContext.master);
        messageLogger = ml.MessageLogger(const.getProjectName(__file__), "Reading from file.....")
        try:
            messageLogger.logInfo("Reading from file.")
            df = spark.read.csv(const.csv_file_project_1, header=True)
            messageLogger.logInfo("File reading finished successfully.")
        except Exception as e:
            messageLogger.logError("unable to read the file, exception occurred: " + str(e.__class__) + "occurred.")
            # print("unable to read the file, exception occurred: " + str(e.__class__) + "occurred.")
        if df.count() > 0:
            messageLogger.logInfo("Number of records in file: "+str(df.count()))

    # df.write.csv("C:/Users/Amit/Documents/Projects/Pyspark/DataAnalytics/resources/write_csv_file.csv", header=True)
