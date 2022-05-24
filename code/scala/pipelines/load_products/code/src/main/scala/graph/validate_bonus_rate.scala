package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object validate_bonus_rate {
  def apply(spark: SparkSession, in0: DataFrame): DataFrame = {
    if (in0.filter(col("bonus_rate") < 0).count() > 0){
        throw new RuntimeException("Found negative bonus rate")
    }
    val out0 = in0
    out0
  }

}
