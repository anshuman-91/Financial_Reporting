from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def schema(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("acc_id").cast(IntegerType()).alias("acc_id"), 
        col("person_id").cast(IntegerType()).alias("person_id"), 
        col("product_id").cast(IntegerType()).alias("product_id"), 
        col("balance").cast(DoubleType()).alias("balance"), 
        col("business_date").cast(DateType()).alias("business_date"), 
        current_timestamp().alias("import_ts")
    )
