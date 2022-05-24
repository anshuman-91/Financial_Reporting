from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Target_1(spark: SparkSession, in0: DataFrame):
    if Config.fabricName == "recipes_fabric":
        in.write\
            .option("header", True)\
            .option("sep", ",")\
            .mode("append")\
            .option("separator", ",")\
            .option("header", True)\
            .csv("dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/acc_status/bronze/")
    elif Config.fabricName == "anshuman2":
        in.write\
            .option("header", True)\
            .option("sep", ",")\
            .mode("overwrite")\
            .option("separator", ",")\
            .option("header", True)\
            .csv("dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/acc_status/bronze/")
    elif Config.fabricName == "anshuman":
        in.write\
            .option("header", True)\
            .option("sep", ",")\
            .mode("overwrite")\
            .option("separator", ",")\
            .option("header", True)\
            .csv("dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/acc_status/bronze/")
    else:
        raise Exception("No valid dataset present to read fabric")
