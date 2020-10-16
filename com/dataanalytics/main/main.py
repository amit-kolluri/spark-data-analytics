from pyspark import SparkContext, SQLContext
import com.dataanalytics.utility.messageLogger as ml


def main():
    __file_name = __file__
    testSparkInstallation()
    messageLogger = ml.MessageLogger(__file_name,"Logger Started")
    messageLogger.loginfo()


def testSparkInstallation():
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    nums = sc.parallelize([10, 2453, 3212, 443])
    print(nums.take(2))
    print("Spark Installed Succesfully")


if __name__ == "__main__":
    main()
