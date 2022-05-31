package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object join_transactions {

  def apply(spark: SparkSession, in0: DataFrame, in1: DataFrame): DataFrame =
    in0
      .as("in0")
      .join(in1.as("in1"),
            (col("in0.acc_id") === col("in1.acc_id"))
              .and(col("in0.business_date") === col("in1.business_date")),
            "left_outer"
      )
      .select(
        col("in0.acc_id").as("acc_id"),
        col("in0.business_date").as("business_date"),
        col("in0.person_id").as("person_id"),
        col("in0.product_id").as("product_id"),
        col("in0.balance").as("balance"),
        col("in1.total_inflows").as("total_inflows"),
        col("in1.total_outflows").as("total_outflows"),
        col("in1.interest_inflow").as("interest_inflow")
      )

}
