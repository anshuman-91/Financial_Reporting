package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._

object acc_status_silver {

  def apply(spark: SparkSession, in: DataFrame): Unit = {
    Config.fabricName match {
      case "recipes_fabric" =>
        in.write
          .format("csv")
          .option("header", true)
          .option("sep",    ",")
          .mode("error")
          .save(
            "dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/acc_status/silver"
          )
      case "anshuman2" =>
        in.write
          .format("parquet")
          .mode("append")
          .partitionBy("business_date", "import_ts")
          .save(
            "dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/acc_status/silver/"
          )
      case "anshuman" =>
        in.write
          .format("parquet")
          .mode("append")
          .partitionBy("business_date", "import_ts")
          .save(
            "dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/acc_status/silver/"
          )
      case _ =>
        throw new Exception("No valid dataset present to read fabric")
    }
  }

}
