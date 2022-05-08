from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def latest_data(spark: SparkSession, in0: DataFrame) -> DataFrame:
    w = Window.partitionBy('acc_id').orderBy(col('business_date').desc())
    out0 = in0.withColumn('row_n', row_number().over(w)).filter(col('row_n') == 1).drop('row_n')

    return out0
