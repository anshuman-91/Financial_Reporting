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
                StructField(
                  "addresses",
                  ArrayType(
                    StructType(
                      Array(
                        StructField("address_line1", StringType, true),
                        StructField("address_line2", StringType, true),
                        StructField("postal_code",   StringType, true),
                        StructField("type",          StringType, true)
                      )
                    ),
                    true
                  ),
                  true
                ),
                StructField("email",      StringType, true),
                StructField("id",         LongType,   true),
                StructField("name",       StringType, true),
                StructField("updated_at", StringType, true)
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
