package graph.reconcile

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object unmatched {

  def apply(spark: SparkSession, in: DataFrame): DataFrame =
    in.filter(col("balance") =!= col("signed_tran_amount"))

}
