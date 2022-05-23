from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *
from job.graph import *

def pipeline(spark: SparkSession) -> None:
    df_dummy = dummy(spark)
    df_acc_status_2 = acc_status_2(spark, df_dummy)
    df_transactions = transactions(spark, df_dummy)
    Target_1(spark, df_transactions)
    df_acc_status_1 = acc_status_1(spark, df_dummy)
    acc_tgt_1(spark, df_acc_status_1)
    acc_tgt_2(spark, df_acc_status_2)
    df_people = people(spark, df_dummy)
    Target_2(spark, df_people)

def main():
    Utils.initializeFromArgs(Utils.parseArgs())
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    initialize(spark)
    pipeline(spark)

if __name__ == "__main__":
    main()
