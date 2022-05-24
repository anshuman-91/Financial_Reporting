import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._
import graph._
import graph.reconcile

object Main {

  def apply(spark: SparkSession): Unit = {
    val df_load       = load(spark)
    val df_null_check = null_check(spark, df_load)
    val df_dedup      = dedup(spark,      df_null_check)
    val df_import_ts  = import_ts(spark,  df_dedup)
    val df_dvr        = dvr(spark,        df_import_ts)
    reconcile.apply(spark, df_dvr)
    Target_1(spark,        df_dvr)
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
