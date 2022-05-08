from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def save(spark: SparkSession, in0: DataFrame):

    if Config.fabricName == "anshuman2":
        in0.write\
            .format("parquet")\
            .mode("append")\
            .partitionBy("business_date", "import_ts")\
            .save("dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/acc_status/silver/")
    else:
        raise Exception("No valid dataset present to read fabric")

    if Config.fabricName == "anshuman":
        in0.write\
            .format("parquet")\
            .mode("append")\
            .partitionBy("business_date", "import_ts")\
            .save("dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/acc_status/silver/")
    else:
        raise Exception("No valid dataset present to read fabric")
