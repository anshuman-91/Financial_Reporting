package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._

object Target_1 {

  def apply(spark: SparkSession, in: DataFrame): Unit = {
    Config.fabricName match {
      case "recipes_fabric" =>
        in.write
          .format("csv")
          .option("header", true)
          .option("sep",    ",")
          .mode("append")
          .save(
            "dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/transactions/bronze/"
          )
      case "anshuman2" =>
        in.write
          .format("parquet")
          .save(
            "dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/transactions/bronze/"
          )
      case _ =>
        throw new Exception("No valid dataset present to read fabric")
    }
  }

}
