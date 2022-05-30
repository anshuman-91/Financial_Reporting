package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object flatten {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.withColumn("addresses", explode_outer(col("addresses")))
      .select(
        col("id"),
        col("email"),
        col("name"),
        col("updated_at"),
        col("addresses.address_line1").as("address_line1"),
        col("addresses.address_line2").as("address_line2"),
        col("addresses.postal_code").as("postal_code"),
        col("addresses.type").as("type")
      )

}
