package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object Join_products {

  def apply(spark: SparkSession, in0: DataFrame, in1: DataFrame): DataFrame =
    in0
      .as("in0")
      .join(in1.as("in1"),
            (col("in0.product_id") === col("in1.id"))
              .and(col("in0.business_date") === col("in1.business_date")),
            "inner"
      )
      .select(
        col("in0.acc_id").as("acc_id"),
        col("in0.business_date").as("business_date"),
        col("in0.person_id").as("person_id"),
        col("in0.product_id").as("product_id"),
        col("in0.balance").as("balance"),
        col("in0.total_inflows").as("total_inflows"),
        col("in0.total_outflows").as("total_outflows"),
        col("in0.interest_inflow").as("interest_inflow"),
        col("in1.name").as("product_name"),
        col("in1.slug").as("product_slug"),
        col("in1.bonus_rate").as("product_bonus_rate"),
        col("in1.lock_in_period").as("product_lock_in_period")
      )

}
