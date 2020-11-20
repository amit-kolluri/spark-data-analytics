import com.dataanalytics.utility.constants as const
import com.dataanalytics.utility.messageLogger as ml
import com.dataanalytics.utility.processFileOps as pf
from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession
import com.dataanalytics.utility.constants as constants

from pyspark.sql.types import StructType


class ReadCsv:

    def __init__(self, msg):
        self.msg = msg

    def readFromCsv(self, spark):
        print("Reading from CSV")

        sqlContext = SQLContext(sc)

        schema = StructType([])
        df = sqlContext.createDataFrame(sc.emptyRDD(), schema)
        print("First SparkContext:")
        print("APP Name :".format(spark.sparkContext.appName))
        print("Master :" + spark.sparkContext.master)
        messageLogger = ml.MessageLogger(const.getProjectName(__file__), "Reading from file.....")
        try:
            messageLogger.logInfo("Reading from CSV file.")
            df = spark.read.csv(const.csv_file_project_1, inferSchema=True, header=True)
            messageLogger.logInfo("File reading finished successfully.")
        except Exception as e:
            messageLogger.logError("unable to read the file, exception occurred: " + str(e.__class__) + "occurred.")
        if df.count() > 0:
            messageLogger.logInfo("Number of records in file: " + str(df.count()))

        # Display Data Frame Results
        #         processedData = pf.ProcessedData()
        #         processedData.processOutput("hellodcddd")
        # df.select('*').show()  # 100, False)

        # Data Frame Filter Statements
        # df.filter(df['eq_site_limit'] == 0).select('*').show()
        df.filter (df['eq_site_limit'] == 0 & df['hu_site_limit'] > 20000).select('*').show()

        # Dataframe

        # df.write.csv("C:/Users/Amit/Documents/Projects/Pyspark/DataAnalytics/resources/write_csv_file.csv", header=True)4296688744
