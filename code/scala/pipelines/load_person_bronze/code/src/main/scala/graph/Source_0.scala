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
          .format("json")
          .option("multiLine",           true)
          .option("recursiveFileLookup", true)
          .load(
            "dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/external/person/"
          )
      case _ =>
        throw new Exception("No valid dataset present to read fabric")
    }
  }

}
