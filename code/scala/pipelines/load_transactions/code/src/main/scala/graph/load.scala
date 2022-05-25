package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._

object load {

  def apply(spark: SparkSession): DataFrame = {
    Config.fabricName match {
      case "anshuman2" =>
        spark.read
          .format("parquet")
          .option("mergeSchema", true)
          .schema(
            StructType(
              Array(
                StructField("acc_id",        IntegerType,   true),
                StructField("tran_id",       StringType,    true),
                StructField("business_date", DateType,      true),
                StructField("tran_amount",   DoubleType,    true),
                StructField("tran_type",     StringType,    true),
                StructField("tran_ts",       TimestampType, true)
              )
            )
          )
          .load(
            "dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/transactions/bronze/"
          )
      case "recipes_fabric" =>
        spark.read
          .format("parquet")
          .load(
            "dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/transactions/bronze/"
          )
      case _ =>
        throw new Exception("No valid dataset present to read fabric")
    }
  }

}
