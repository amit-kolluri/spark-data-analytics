import com.dataanalytics.utility.data_access_object as dao
import com.dataanalytics.utility.constants as const
import com.dataanalytics.utility.messageLogger as ml

class ProcessDBOps:
    def __init__(self, spark, db_instance):
        self.db_instance = db_instance
        self.spark = spark
        self.messageLogger = ml.MessageLogger(const.getProjectName(__file__), "Logger Started")
        self.messageLogger.logInfo("Processing read and write operations from Database")

    def writeToDB(self, df, write_mode, table_name):
        try:
            self.messageLogger.logInfo("Writing to "+self.db_instance+" Database instance")
            data_point = dao.DataAccessObj(self.spark)
            db_properties, db_url = data_point.configureDBproperties(database_instance=self.db_instance)
            df.write.jdbc(url=db_url, table=table_name, mode=write_mode, properties=db_properties)
            self.messageLogger.logInfo("successfully loaded data  to "+self.db_instance+" Database")
        except Exception as e:
            self.messageLogger.logError("!!!Error writing to "+self.db_instance+" Database instance ::: " + str(e.__class__) + "occurred.")

    def readFromDB(self, table_name):
        df = const.createEmptyDF(self.spark)
        try:
            self.messageLogger.logInfo("Reading from "+self.db_instance+" Database instance")
            data_point = dao.DataAccessObj(self.spark)
            db_properties, db_url = data_point.configureDBproperties(database_instance=self.db_instance)
            df = self.spark.read.jdbc(url=db_url, table=table_name, properties=db_properties)
            self.messageLogger.logInfo("successfully loading data from " + self.db_instance + " Database")
        except Exception as e:
            self.messageLogger.logError("!!!Error reading to "+self.db_instance+" Database instance ::: " + str(e.__class__) + "occurred.")

        return df

    def createDBtable(self, create_table_stmt):
        data_point = dao.DataAccessObj(self.spark)
        db_properties, db_url = data_point.configureDBproperties(database_instance=self.db_instance)
        # self.spark.sql(create_table_stmt)
        self.spark.sql('create table if not exists ndame_details(fn string,last_n string)')
