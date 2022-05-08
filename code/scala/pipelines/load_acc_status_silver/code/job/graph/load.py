from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def load(spark: SparkSession) -> DataFrame:

    if Config.fabricName == "anshuman2":
        return spark.read\
            .schema(
              StructType([
                StructField("acc_id", StringType(), True), StructField("person_id", StringType(), True), StructField("product_id", StringType(), True), StructField("business_date", StringType(), True), StructField("balance", StringType(), True)
            ])
            )\
            .option("header", True)\
            .option("sep", ",")\
            .csv("dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/acc_status/bronze/")
    else:
        raise Exception("No valid dataset present to read fabric")

    if Config.fabricName == "anshuman":
        return spark.read\
            .schema(
              StructType([
                StructField("acc_id", StringType(), True), StructField("person_id", StringType(), True), StructField("product_id", StringType(), True), StructField("business_date", StringType(), True), StructField("balance", StringType(), True)
            ])
            )\
            .option("header", True)\
            .option("sep", ",")\
            .csv("dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/acc_status/bronze/")
    else:
        raise Exception("No valid dataset present to read fabric")
