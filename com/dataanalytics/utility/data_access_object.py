import configparser

import spark as spark

from pyspark.sql import SparkSession


class DataAccessObj:
    def __init__(self, spark):
        self.spark = spark
        self.db_properties ={}

    def configureDBproperties(self, database_instance):
        config = configparser.ConfigParser()
        config.read("../db_properties.ini")
        db_prop = config['local']
        db_url = db_prop['url']
        self.db_properties['user'] = db_prop['username']
        self.db_properties['password'] = db_prop['password']
        self.db_properties['url'] = db_url
        self.db_properties['driver'] = db_prop['driver']
        return self.db_properties, db_url
