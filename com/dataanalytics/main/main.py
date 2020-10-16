import com.dataanalytics.utility.constants as const
import com.dataanalytics.utility.messageLogger as ml
import com.dataanalytics.services.project1 as p1
from pyspark import SparkContext, SQLContext


def main():
    # testSparkInstallation()
    messageLogger = ml.MessageLogger(const.getProjectName(__file__), "Logger Started")
    messageLogger.logInfo()
    project1 = p1.ReadCsv(messageLogger)

    const.updateJobRunId()


def testSparkInstallation():
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    nums = sc.parallelize([10, 2453, 3212, 443])
    print(nums.take(2))
    print("Spark Installed Succesfully")


if __name__ == "__main__":
    main()
