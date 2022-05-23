package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
package object get_latest_3 {

  def apply(spark: SparkSession, in0: DataFrame): DataFrame = {
    val df_WindowFunction_1_1_1_1 = WindowFunction_1_1_1_1(spark, in0)
    val df_Filter_1_1_1_1         = Filter_1_1_1_1(spark,         df_WindowFunction_1_1_1_1)
    val df_SchemaTransform_1_1_1_1 =
      SchemaTransform_1_1_1_1(spark, df_Filter_1_1_1_1)
    df_SchemaTransform_1_1_1_1
  }

}
