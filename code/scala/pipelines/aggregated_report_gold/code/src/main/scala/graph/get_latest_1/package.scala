package graph

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
package object get_latest_1 {

  def apply(spark: SparkSession, in0: DataFrame): DataFrame = {
    val df_add_row_num         = add_row_num(spark,         in0)
    val df_Filter_1_1          = Filter_1_1(spark,          df_add_row_num)
    val df_SchemaTransform_1_1 = SchemaTransform_1_1(spark, df_Filter_1_1)
    df_SchemaTransform_1_1
  }

}
