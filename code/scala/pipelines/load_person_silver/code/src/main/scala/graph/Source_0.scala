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
      case "anshuman2" =>
        spark.read
          .format("json")
          .option("multiLine", true)
          .option("mode",      "FAILFAST")
          .schema(
            StructType(
              Array(
                StructField("id",    IntegerType, false),
                StructField("email", StringType,  false),
                StructField("name",  StringType,  true),
                StructField(
                  "addresses",
                  ArrayType(
                    StructType(
                      Array(
                        StructField("address_line1", StringType, false),
                        StructField("address_line2", StringType, true),
                        StructField("postal_code",   StringType, false),
                        StructField("type",          StringType, true)
                      )
                    ),
                    true
                  ),
                  true
                ),
                StructField("updated_at", TimestampType, false)
              )
            )
          )
          .load(
            "dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/person/bronze/"
          )
      case "anshuman" =>
        spark.read
          .format("csv")
          .option("header", true)
          .option("sep",    ",")
          .load(
            "dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/person/bronze/"
          )
      case "recipes_fabric" =>
        spark.read
          .format("json")
          .schema(
            StructType(
              Array(
                StructField("addresses",  StringType, true),
                StructField("email",      StringType, true),
                StructField("id",         LongType,   true),
                StructField("name",       StringType, true),
                StructField("updated_at", StringType, true)
              )
            )
          )
          .load(
            "dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/person/bronze/"
          )
      case _ =>
        throw new Exception("No valid dataset present to read fabric")
    }
  }

}
