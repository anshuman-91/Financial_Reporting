from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def acc_tgt_2(spark: SparkSession, in0: DataFrame):
    if Config.fabricName == "recipes_fabric":
        in0.write\
            .option("header", True)\
            .option("sep", ",")\
            .mode("overwrite")\
            .option("separator", ",")\
            .option("header", True)\
            .csv("dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/external/acc_status/acc_status_2022-05-05.csv")
    else:
        raise Exception("No valid dataset present to read fabric")
