package graph.reconcile

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._

object balances_1 {

  def apply(spark: SparkSession): DataFrame = {
    Config.fabricName match {
      case "anshuman2" =>
        spark.read
          .format("parquet")
          .load(
            "dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/acc_status/silver/"
          )
      case "anshuman" =>
        spark.read
          .format("parquet")
          .load(
            "dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/acc_status/silver/"
          )
      case _ =>
        throw new Exception("No valid dataset present to read fabric")
    }
  }

}
