from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *
from job.graph import *

def pipeline(spark: SparkSession) -> None:
    df_load = load(spark)
    df_schema = schema(spark, df_load)
    df_latest_data = latest_data(spark, df_schema)
    save(spark, df_latest_data)

def main():
    Utils.initializeFromArgs(Utils.parseArgs())
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    pipeline(spark)

if __name__ == "__main__":
    main()
