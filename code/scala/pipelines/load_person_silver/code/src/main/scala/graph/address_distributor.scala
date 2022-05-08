package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object address_distributor {

  def apply(spark: SparkSession, in: DataFrame): (DataFrame, DataFrame) =
    (in.filter(col("type") === lit("PRIMARY")),
     in.filter(col("type") === lit("ALTERNATE"))
    )

}
