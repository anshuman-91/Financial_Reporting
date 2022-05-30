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
    val df_load_bronze = load_bronze(spark)
    val df_dedup       = dedup.apply(spark, df_load_bronze)
    val df_flatten     = flatten(spark,     df_dedup)
    val (df_address_distributor_out0, df_address_distributor_out1) =
      address_distributor(spark, df_flatten)
    val df_compress = compress(spark, df_address_distributor_out1)
    val df_collect  = collect(spark,  df_compress)
    val df_primary_join_alternate =
      primary_join_alternate(spark, df_address_distributor_out0, df_collect)
    val df_null_check     = null_check(spark,     df_primary_join_alternate)
    val df_import_ts      = import_ts(spark,      df_null_check)
    val df_validate_email = validate_email(spark, df_import_ts)
    append_silver(spark, df_validate_email)
  }

  def main(args: Array[String]): Unit = {
    import config._
    ConfigStore.Config = ConfigurationFactoryImpl.fromCLI(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Prophecy Pipeline")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
    apply(spark)
  }

}
