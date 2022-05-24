from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def transactions(spark: SparkSession, in0: DataFrame) -> DataFrame:
    transactions_df = spark.createDataFrame(
        data = [("1", "a1x", "2022-05-05", "200.00", "CREDIT", "2022-05-05T01:30:00.184-05:00"),
         ("1", "a1y", "2022-05-05", "110.00", "DEBIT", "2022-05-05T09:45:00.193-05:00"),
         ("1", "a1z", "2022-05-05", "10.00", "INTEREST", "2022-05-05T12:00:00.564-05:00"),
         ("2", "a2a", "2022-05-05", "50.00", "CREDIT", "2022-05-05T11:31:22.231-05:00"),
         ("2", "a2b", "2022-05-05", "200.00", "CREDIT", "2022-05-05T05:04:21.871-05:00"),
         ("2", "a2c", "2022-05-05", "100.00", "DEBIT", "2022-05-05T12:05:11.135-05:00"),
         ("3", "a2d", "2022-05-05", "449.50", "CREDIT", "2022-05-05T15:05:15.154-05:00"),
         ("3", "a2e", "2022-05-05", "449.50", "CREDIT", "2022-05-05T18:05:31.130-05:00"),
         ("3", "a2f", "2022-05-05", "100.00", "INTEREST", "2022-05-05T22:45:55.116-05:00"),],
        schema = 'acc_id string,tran_id string,business_date string,tran_amount string,tran_type string,tran_ts string'
    )
    out0 = transactions_df.coalesce(1)

    return out0
