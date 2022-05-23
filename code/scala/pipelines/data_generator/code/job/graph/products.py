from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def products(spark: SparkSession, in0: DataFrame) -> DataFrame:
    product_json_1 = '''{"id":1,"name":"Fixed Term","properties":{"bonus_rate":1.512,"lock_in_period":1},"slug":"Fixed Duration 1 Year","updated_at":"2022-05-05T12:00:00.000-05:00"}'''
    product_json_2 = '''{"id":2,"name":"Savings","properties":{"bonus_rate":1.512},"slug":"Online Savings Account","updated_at":"2022-05-05T12:00:00.000-05:00"}'''
    products_df = spark.read.json(sc.parallelize([product_json_1, product_json_2]))
    out0 = products_df.coalesce(1)

    return out0
