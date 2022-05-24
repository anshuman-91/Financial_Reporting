from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Source_0(spark: SparkSession) -> DataFrame:
    if Config.fabricName == "recipes_fabric":
        return spark.read\
            .schema(
              StructType([
                StructField("acc_id", IntegerType(), True), StructField("person_id", StringType(), True), StructField("product_id", StringType(), True), StructField("business_date", DateType(), True), StructField("balance", DoubleType(), True)
            ])
            )\
            .option("header", True)\
            .option("sep", ",")\
            .option("recursiveFileLookup", True)\
            .csv("dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/external/acc_status/")
    else:
        raise Exception("No valid dataset present to read fabric")
