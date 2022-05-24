package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._

object Source_0 {

  def apply(spark: SparkSession): DataFrame = {
    Config.fabricName match {
      case "recipes_fabric" =>
        spark.read
          .format("parquet")
          .load(
            "dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/external/transactions/transactions_2022-05-05.parquet"
          )
      case _ =>
        throw new Exception("No valid dataset present to read fabric")
    }
  }

}
