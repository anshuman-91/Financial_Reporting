from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def dummy(spark: SparkSession) -> DataFrame:
    if Config.fabricName == "recipes_fabric":
        return spark.read\
            .schema(
              StructType([
                StructField("acc_id", StringType(), True), StructField("person_id", StringType(), True), StructField("product_id", StringType(), True), StructField("business_date", StringType(), True), StructField("balance", StringType(), True)
            ])
            )\
            .option("header", True)\
            .option("sep", ",")\
            .csv("dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/acc_status/bronze/")
    elif Config.fabricName == "anshuman2":
        return spark.read\
            .schema(
              StructType([
                StructField("acc_id", IntegerType(), False), StructField("person_id", IntegerType(), False), StructField("product_id", IntegerType(), False), StructField("business_date", DateType(), False), StructField("balance", DoubleType(), False)
            ])
            )\
            .option("header", True)\
            .option("sep", ",")\
            .csv("dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/acc_status/bronze/")
    elif Config.fabricName == "anshuman":
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
