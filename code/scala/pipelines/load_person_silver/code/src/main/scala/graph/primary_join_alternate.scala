package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object primary_join_alternate {

  def apply(spark: SparkSession, in0: DataFrame, in1: DataFrame): DataFrame =
    in0
      .as("in0")
      .join(in1.as("in1"), col("in0.id") === col("in0.id"), "inner")
      .select(
        col("in0.id").as("id"),
        col("in0.email").as("email"),
        col("in0.name").as("name"),
        col("in0.updated_at").as("updated_at"),
        col("in0.address_line1").as("primary_address_line1"),
        col("in0.address_line2").as("primary_address_line2"),
        col("in0.postal_code").as("primary_postal_code"),
        col("in1.address_string").as("alternate_addresses")
      )

}
