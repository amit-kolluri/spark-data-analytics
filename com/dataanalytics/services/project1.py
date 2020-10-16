import com.dataanalytics.utility.constants as constants
import com.dataanalytics.utility.messageLogger as lg
from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


def runProject1():
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    spark = SparkSession \
        .builder \
        .appName("how to read csv file") \
        .getOrCreate()
    schema = StructType([])
    df = sqlContext.createDataFrame(sc.emptyRDD(), schema)

    current_file_name = __file__
    *middle, current_file_name = current_file_name.split("\\")

    lg.loginfo(current_file_name, "Reading from file.....")
    try:
        df = spark.read.csv(constants.csv_file_project_1, header=True)
    except Exception as e:
        print("unable to read the file, exception occured: ", e.__class__, "occurred.")

    if df.count() > 0:
        print("Number of records in file: ", df.count())

# df.write.csv("C:/Users/Amit/Documents/Projects/Pyspark/DataAnalytics/resources/write_csv_file.csv", header=True)
