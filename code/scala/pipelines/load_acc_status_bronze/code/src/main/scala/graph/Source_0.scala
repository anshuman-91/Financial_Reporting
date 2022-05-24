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
          .format("csv")
          .option("header",              true)
          .option("sep",                 ",")
          .option("recursiveFileLookup", true)
          .schema(
            StructType(
              Array(
                StructField("acc_id",        StringType, true),
                StructField("person_id",     StringType, true),
                StructField("product_id",    StringType, true),
                StructField("business_date", StringType, true),
                StructField("balance",       StringType, true)
              )
            )
          )
          .load(
            "dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/external/acc_status/"
          )
      case _ =>
        throw new Exception("No valid dataset present to read fabric")
    }
  }

}
