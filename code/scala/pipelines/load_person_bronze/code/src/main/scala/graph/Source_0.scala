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
          .schema(
            StructType(
              Array(
                StructField("addresses",
                            ArrayType(StructType(Array()), true),
                            true
                ),
                StructField("email",      StringType,    true),
                StructField("id",         StringType,    true),
                StructField("name",       StringType,    true),
                StructField("updated_at", TimestampType, true)
              )
            )
          )
          .load(
            "dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/external/people/"
          )
      case _ =>
        throw new Exception("No valid dataset present to read fabric")
    }
  }

}
