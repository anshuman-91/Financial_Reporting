package graph.reconcile

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object join_tran_balance {

  def apply(spark: SparkSession, in0: DataFrame, in1: DataFrame): DataFrame =
    in0
      .as("in0")
      .join(in1.as("in1"),
            (col("in0.acc_id") === col("in1.acc_id"))
              .and(col("in0.business_date") === col("in1.business_date")),
            "inner"
      )
      .select(
        col("in0.acc_id").as("acc_id"),
        col("in0.business_date").as("business_date"),
        col("in0.signed_tran_amount").as("signed_tran_amount"),
        col("in1.bal_change").as("bal_change")
      )

}
