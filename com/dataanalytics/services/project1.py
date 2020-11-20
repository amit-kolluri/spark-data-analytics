import com.dataanalytics.utility.constants as const
import com.dataanalytics.utility.messageLogger as ml
import com.dataanalytics.utility.createSqlQueries as sqlQueries
import com.dataanalytics.utility.processDBops as dbops
from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

import com.dataanalytics.utility.processFileOps as dataO


class DataFrameOperations:

    def __init__(self, spark):
        self.spark = spark
        self.messageLogger = ml.MessageLogger(const.getProjectName(__file__), "Logger Started")
        self.messageLogger.logInfo("Processing read and write operations from files")

    def readFromCsv(self):
        print("Reading from CSV")
        messageLogger = ml.MessageLogger(const.getProjectName(__file__), "Reading from file.....")
        readDataFromCsv = dataO.ProcessFileOps(self.spark)
        df= readDataFromCsv.readCSV(const.csv_file_project_1)
        if df.count() > 0:
            messageLogger.logInfo("Number of records in file: " + str(df.count()))
        return df


    def dataFrameExamples(self, df):
        # Data Frame Operations:

        #print schema
        df.printSchema()

        #count of records
        df.count()

        #Filter, Limit
        filter1 = df.filter(df.eq_site_limit >10000).limit(5)
        filter1.show()

        filter2 = df.filter(df.eq_site_limit == 0).limit(5)
        filter2.show()

        #union
        df3 = filter1.union(filter2)
        df3.show()

        #select columns
        df.select('policyID','tiv_2011').show()

        #change the column data type or update existing columns
        df4 = df.withColumn("hu_site_deductible",df.hu_site_deductible.cast("Decimal"))
        df4.printSchema()

        #Joins
        df5 = df.withColumn("hu_site_deductible",df.hu_site_deductible*10+1)
        df.join(df5, df.policyID == df5.policyID, 'inner').select(df.policyID, df.hu_site_deductible, df5.hu_site_deductible).show()

        #order by
        df.select('policyID').sort(df.policyID.desc()).show()

        #distinct records
        df.distinct().show()

        #collection of data in array format
        dataCollect = df.collect()
        # print(dataCollect)

        #Map Functions (Source::internet)
        arrayData = [
            ('James', ['Java', 'Scala'], {'hair': 'black', 'eye': 'brown'}),
            ('Michael', ['Spark', 'Java', None], {'hair': 'brown', 'eye': None}),
            ('Robert', ['CSharp', ''], {'hair': 'red', 'eye': ''}),
            ('Washington', None, None),
            ('Jefferson', ['1', '2'], {})
            ]
        df9 = self.spark.createDataFrame(data=arrayData, schema=['name', 'knownLanguages', 'properties'])

        from pyspark.sql.functions import explode
        df9 = df9.select(df9.name, explode(df9.knownLanguages))
        df9.printSchema()
        df9.show()

    def writeToDB(self, df):
        # To Create Tables in Postgres DB
        # columns_list = df.columns
        # table_name = "postgres.spark_demo.first_table"
        # sqlquery = sqlQueries.SqlQueries(self.spark, columns_list, table_name)
        # create_table_stmt= sqlquery.createTableInDB()
        # data_create = dbops.ProcessDBOps(self.spark, "local")
        # data_create.createDBtable(create_table_stmt)
        # print(create_table_stmt)
        # df.write.csv(r"C:\Users\amitk\Documents\projects\python\spark-data-analytics\resources", header=True)
        table_name = "spark_demo.insurance_sample"
        write_mode = "overWrite"
        data_create = dbops.ProcessDBOps(self.spark, "local")
        data_create.writeToDB(df, write_mode, table_name)
        # print(create_table_stmt)


