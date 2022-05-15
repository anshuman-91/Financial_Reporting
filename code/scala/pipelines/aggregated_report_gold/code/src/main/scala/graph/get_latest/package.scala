package graph

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
package object get_latest {

  def apply(spark: SparkSession, in0: DataFrame): DataFrame = {
    val df_WindowFunction_1  = WindowFunction_1(spark,  in0)
    val df_Filter_1          = Filter_1(spark,          df_WindowFunction_1)
    val df_SchemaTransform_1 = SchemaTransform_1(spark, df_Filter_1)
    df_SchemaTransform_1
  }

}
