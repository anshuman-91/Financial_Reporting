import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._
import graph._
import graph.get_latest

object Main {

  def apply(spark: SparkSession): Unit = {
    val df_Source_1   = Source_1(spark)
    val df_acc_status = acc_status(spark)
    val df_get_latest = get_latest.apply(spark, df_acc_status)
    val df_Join_1     = Join_1(spark,           df_get_latest)
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
