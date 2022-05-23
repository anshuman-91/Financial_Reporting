from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def acc_status_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    acc_status_df_first = spark.createDataFrame(
        data = [("1", "A", "P1", "2022-05-04", "0.00"), ("2", "B", "P1", "2022-05-04", "0.00"),
         ("3", "A", "P2", "2022-05-04", "0.00"),],
        schema = 'acc_id string,person_id string,product_id string,business_date string,balance string'
    )
    out0 = acc_status_df_first.coalesce(1)

    return out0
