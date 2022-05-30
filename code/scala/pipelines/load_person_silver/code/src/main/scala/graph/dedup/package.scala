package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
package object dedup {

  def apply(spark: SparkSession, in0: DataFrame): DataFrame = {
    val df_add_row_num  = add_row_num(spark,  in0)
    val df_row_num_eq1  = row_num_eq1(spark,  df_add_row_num)
    val df_drop_row_num = drop_row_num(spark, df_row_num_eq1)
    df_drop_row_num
  }

}
