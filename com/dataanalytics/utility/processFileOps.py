import json
import logging
import os

import com.dataanalytics.utility.constants as const
import com.dataanalytics.utility.messageLogger as ml

import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


class ProcessFileOps:
    def __init__(self, spark):
        self.spark = spark
        self.messageLogger = ml.MessageLogger(const.getProjectName(__file__), "Logger Started")
        self.messageLogger.logInfo("Processing read and write operations from files")

    def processCSVoutput(self, df, file_name):
        fn_csv = const.processedFile + file_name +"_csv"
        msg = self.createFolder(fn_csv)
        self.messageLogger.logInfo(msg)
        try:
            self.messageLogger.logInfo("starting csv write operation")
            # pandasDF = df.toPandas()
            # pandasDF.to_csv(fn_csv, index=False, line_terminator='\n')

            df.write.csv(fn_csv, header=True)
            self.messageLogger.logInfo("successfully loaded data into csv file")
        except Exception as e:
            self.messageLogger.logError("!!!Error writing to csv ::: " + str(e.__class__) + "occurred.")

    def processParquetOutput(self, df, file_name, partion_size, partition_col):
        print(partition_col)
        # fn_parquet = const.processedFile + file_name + ".gzip"
        fn_parquet = const.processedFile + file_name + "_parquet"
        msg = self.createFolder(fn_parquet)
        self.messageLogger.logInfo(msg)
        try:
            self.messageLogger.logInfo("starting parquet write operation")
            # pandasDF = df.toPandas()
            # pandasDF.to_parquet(fn_parquet, compression='gzip')
            df.repartition(partion_size).write.partitionBy(partition_col).parquet(fn_parquet)
            self.messageLogger.logInfo("successfully loaded data into parquet")
        except Exception as e:
            self.messageLogger.logError("!!!Error writing to parquet ::: " + str(e.__class__) + "occurred.")
        # Test Parquet Data
        # a = pd.read_parquet(self.fn_parquet)
        # print(a.to_string(index=False))
        # pandasDF.to_parquet(self.fn_parquet,compression = 'gzip') #, index=False, line_terminator='\n')

    def processJsonOutput(self, df, file_name):
        # fn_json = open(const.processedFile + file_name + ".json", 'w+')
        fn_json = const.processedFile + file_name + "_json"
        msg = self.createFolder(fn_json)
        self.messageLogger.logInfo(msg)
        try:
            self.messageLogger.logInfo("starting Json write operation")
            # pandasDF = df.toPandas().reset_index()
            # pandasDF.to_json(fn_json)
            df.write.json(fn_json)
            self.messageLogger.logInfo("successfully loaded data into json")
        except Exception as e:
            self.messageLogger.logError("!!!Error writing to Json  ::: " + str(e.__class__) + "occurred.")

    def readCSV(self, file_name):
        df = const.createEmptyDF(self.spark)
        try:
            self.messageLogger.logInfo("Reading from CSV file.")
            df = self.spark.read.csv(file_name, header=True)
            self.messageLogger.logInfo("File reading finished successfully.")
        except Exception as e:
            self.messageLogger.logError(
                "unable to read the file, exception occurred: " + str(e.__class__) + "occurred.")
        return df

    def createFolder(self, fp):
        msg = ""
        try:
            import shutil
            shutil.rmtree(fp)
            msg = "folder successfully deleted"
        except Exception as e:
            msg = "folder not found"

    def writeTextFile(self, file_name, text):
        fp = open(file_name,'w+')
        fp.write(text)
        fp.close()

