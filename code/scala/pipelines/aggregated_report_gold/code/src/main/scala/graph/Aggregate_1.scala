package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object Aggregate_1 {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.groupBy(col("business_date"), col("acc_id"))
      .agg(
        sum(
          when(col("tran_type") === "CREDIT", col("tran_amount")).otherwise(0)
        ).as("total_inflows"),
        sum(when(col("tran_type") === "DEBIT", col("tran_amount")).otherwise(0))
          .as("total_outflows"),
        sum(
          when(col("tran_type") === "INTEREST", col("tran_amount")).otherwise(0)
        ).as("interest_inflow")
      )

}
