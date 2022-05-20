import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._
import graph._
import graph.get_latest_1
import graph.get_latest
import graph.get_latest_2
import graph.get_latest_3

object Main {

  def apply(spark: SparkSession): Unit = {
    val df_Transactions = Transactions(spark)
    val df_get_latest_1 = get_latest_1.apply(spark, df_Transactions)
    val df_totals       = totals(spark,             df_get_latest_1)
    val df_acc_status   = acc_status(spark)
    val df_get_latest   = get_latest.apply(spark,   df_acc_status)
    val df_join_transactions =
      join_transactions(spark, df_get_latest, df_totals)
    val df_Products     = Products(spark)
    val df_get_latest_2 = get_latest_2.apply(spark, df_Products)
    val df_Join_products =
      Join_products(spark, df_join_transactions, df_get_latest_2)
    val df_People       = People(spark)
    val df_get_latest_3 = get_latest_3.apply(spark, df_People)
    val df_Join_people  = Join_people(spark,        df_Join_products, df_get_latest_3)
    Report(spark, df_Join_people)
  }

  def main(args: Array[String]): Unit = {
    import config._
    ConfigStore.Config = ConfigurationFactoryImpl.fromCLI(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism", "4")
      .enableHiveSupport()
      .getOrCreate()
    apply(spark)
  }

}
