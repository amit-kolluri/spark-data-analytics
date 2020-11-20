import com.dataanalytics.utility.processDBops as dbops
import com.dataanalytics.utility.createSqlQueries as create_sql

import com.dataanalytics.utility.constants as const
import com.dataanalytics.utility.messageLogger as ml


class DatabaseOps:
    def __init__(self, spark):
        self.spark = spark
        self.messageLogger = ml.MessageLogger(const.getProjectName(__file__), "Logger Started")
        self.messageLogger.logInfo("Processing read and write operations from files")

    def write_to_db(self, df):

        # df = self.spark.createDataFrame(data=data, schema=columns)
        create_sql_query = create_sql.SqlQueries(self.spark, df.columns, "spark_demo.test_dataframe")
        create_sql_query.getCreateTableQueryFromDF()
        data_write = dbops.ProcessDBOps(self.spark, "local")
        data_write.writeToDB(df,'append', "spark_demo.test_dataframe")
        df.show()

    def read_from_db(self):
        table_name = "spark_demo.name_details"
        data_read = dbops.ProcessDBOps(self.spark, 'local')
        df = data_read.readFromDB(table_name)
        df.show()



