package graph.dedup

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object dedup_id_N_udpate_ts {

  def apply(spark: SparkSession, in: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.Window
    in.withColumn("row_number",
                  row_number().over(
                    Window
                      .partitionBy("id", "updated_at")
                      .orderBy(lit(1))
                      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
                  )
      )
      .filter(col("row_number") === lit(1))
      .drop("row_number")
  }

}
