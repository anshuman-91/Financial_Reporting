package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object null_check {
  def apply(spark: SparkSession, in0: DataFrame): DataFrame = {
    if (in0.filter(
        col("acc_id").isNull
        or col("tran_amount").isNull
        or col("tran_type").isNull or col("tran_type") === ""
        or col("business_date").isNull
        or col("tran_ts").isNull
        ).count()> 0){
        throw new RuntimeException("Schema Validation Failed")
    }
    val out0 = in0
    out0
    out0
  }

}
