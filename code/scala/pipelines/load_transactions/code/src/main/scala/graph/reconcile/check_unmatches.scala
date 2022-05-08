package graph.reconcile

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object check_unmatches {
  def apply(spark: SparkSession, in0: DataFrame): DataFrame = {
    if (in0.count() > 0){
        throw new RuntimeException("Transactions don't add up to balance")
    }
    val out0 = in0
    out0
  }

}
