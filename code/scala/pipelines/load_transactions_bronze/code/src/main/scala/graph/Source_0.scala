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
          .option("recursiveFileLookup", true)
          .schema(
            StructType(
              Array(
                StructField("acc_id",        StringType, true),
                StructField("tran_id",       StringType, true),
                StructField("business_date", StringType, true),
                StructField("tran_amount",   StringType, true),
                StructField("tran_type",     StringType, true),
                StructField("tran_ts",       StringType, true)
              )
            )
          )
          .load(
            "dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/external/transactions/"
          )
      case _ =>
        throw new Exception("No valid dataset present to read fabric")
    }
  }

}
