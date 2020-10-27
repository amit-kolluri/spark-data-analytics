import com.dataanalytics.utility.constants as const
import com.dataanalytics.utility.messageLogger as ml

from pyspark import SparkContext


def main():
    # testSparkInstallation()
    messageLogger = ml.MessageLogger(const.getProjectName(__file__), "Logger Started")
    messageLogger.logInfo("Logger Started")
    import com.dataanalytics.services.project1 as p1
    project1 = p1.ReadCsv(messageLogger)
    project1.readFromCsv()
    const.updateJobRunId()


def testSparkInstallation():
    sc = SparkContext()
    nums = sc.parallelize([10, 2453, 3212, 443])
    print(nums.take(2))
    print("Spark Installed Succesfully")


if __name__ == "__main__":
    main()
