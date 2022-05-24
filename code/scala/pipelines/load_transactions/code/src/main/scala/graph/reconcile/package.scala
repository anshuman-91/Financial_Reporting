package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
package object reconcile {

  def apply(spark: SparkSession, in0: DataFrame): Unit = {
    val df_balances_1        = balances_1(spark)
    val df_signed_tran_amt_1 = signed_tran_amt_1(spark, in0)
    val df_WindowFunction_1  = WindowFunction_1(spark,  df_balances_1)
    val df_Filter_1          = Filter_1(spark,          df_WindowFunction_1)
    val df_WindowFunction_2  = WindowFunction_2(spark,  df_Filter_1)
    val df_SchemaTransform_1 = SchemaTransform_1(spark, df_WindowFunction_2)
    val df_sum_trans         = sum_trans(spark,         df_signed_tran_amt_1)
    val df_join_tran_balance =
      join_tran_balance(spark, df_sum_trans, df_SchemaTransform_1)
    val df_unmatched       = unmatched(spark,       df_join_tran_balance)
    val df_check_unmatches = check_unmatches(spark, df_unmatched)
  }

}
