package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
package object dedup {

  def apply(spark: SparkSession, in0: DataFrame): DataFrame = {
    val df_orderby_update_ts = orderby_update_ts(spark, in0)
    val df_dedup_id_N_udpate_ts =
      dedup_id_N_udpate_ts(spark, df_orderby_update_ts)
    df_dedup_id_N_udpate_ts
  }

}
