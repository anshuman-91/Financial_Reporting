package graph.reconcile

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object WindowFunction_2 {

  def apply(spark: SparkSession, in: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.{Window, WindowSpec}
    in.withColumn(
      "prev_balance",
      lag(col("balance"), 1).over(
        Window.partitionBy(col("acc_id")).orderBy(col("business_date").asc)
      )
    )
  }

}
