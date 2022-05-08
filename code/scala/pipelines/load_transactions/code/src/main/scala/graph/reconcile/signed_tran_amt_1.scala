package graph.reconcile

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object signed_tran_amt_1 {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.withColumn("signed_tran_amount",
                  when(col("tran_type") === lit("DEBIT"), -col("tran_amount"))
                    .otherwise(col("tran_amount"))
    )

}
