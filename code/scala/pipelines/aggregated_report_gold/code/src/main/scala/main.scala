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
import graph.SubGraph_1

object Main {

  def apply(spark: SparkSession): Unit = {
    val df_Source_1     = Source_1(spark)
    val df_get_latest_1 = get_latest_1.apply(spark, df_Source_1)
    val df_Aggregate_1  = Aggregate_1(spark,        df_get_latest_1)
    val df_Reformat_1   = Reformat_1(spark,         df_Aggregate_1)
    val df_Source_2     = Source_2(spark)
    val df_acc_status   = acc_status(spark)
    val df_get_latest   = get_latest.apply(spark,   df_acc_status)
    val df_Join_1       = Join_1(spark,             df_get_latest, df_Reformat_1)
    val df_SubGraph_1   = SubGraph_1.apply(spark,   df_Source_2)
    val df_Join_2       = Join_2(spark,             df_Join_1,     df_SubGraph_1)
    val df_Reformat_2   = Reformat_2(spark,         df_Join_2)
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
