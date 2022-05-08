package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object flatten_schema {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.select(col("id"),
              col("name"),
              col("slug"),
              col("properties.bonus_rate").as("bonus_rate"),
              col("properties.lock_in_period").as("lock_in_period")
    )

}
