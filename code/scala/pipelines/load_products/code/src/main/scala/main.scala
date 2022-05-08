import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._
import graph._
import graph.dedup

object Main {

  def apply(spark: SparkSession): Unit = {
    val df_Source_0       = Source_0(spark)
    val df_dedup          = dedup.apply(spark,    df_Source_0)
    val df_flatten_schema = flatten_schema(spark, df_dedup)
    val df_null_check     = null_check(spark,     df_flatten_schema)
    val df_import_ts      = import_ts(spark,      df_null_check)
    Target_1(spark, df_import_ts)
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
