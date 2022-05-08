package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object compress {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.select(col("id"),
              col("email"),
              col("name"),
              col("updated_at"),
              expr(
                "concat_ws('$$$', address_line1, address_line2, postal_code)"
              ).as("address_string")
    )

}
