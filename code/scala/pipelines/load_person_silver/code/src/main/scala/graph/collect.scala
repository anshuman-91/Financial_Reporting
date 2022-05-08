package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object collect {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.groupBy(col("id"))
      .agg(
        col("id"),
        first(col("email")).as("email"),
        first(col("name")).as("name"),
        first(col("updated_at")).as("updated_at"),
        collect_list(col("address_string")).as("address_string")
      )

}
