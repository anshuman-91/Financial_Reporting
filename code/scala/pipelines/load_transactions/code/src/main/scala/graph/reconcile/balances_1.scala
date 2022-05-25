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
          .schema(
            StructType(
              Array(
                StructField("acc_id",        IntegerType,   false),
                StructField("person_id",     IntegerType,   false),
                StructField("product_id",    IntegerType,   false),
                StructField("business_date", DateType,      false),
                StructField("balance",       DoubleType,    false),
                StructField("import_ts",     TimestampType, false)
              )
            )
          )
          .load(
            "dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/acc_status/silver/"
          )
      case "anshuman" =>
        spark.read
          .format("parquet")
          .schema(
            StructType(
              Array(
                StructField("acc_id",        IntegerType,   false),
                StructField("person_id",     IntegerType,   false),
                StructField("product_id",    IntegerType,   false),
                StructField("balance",       DoubleType,    false),
                StructField("business_date", DateType,      false),
                StructField("import_ts",     TimestampType, false)
              )
            )
          )
          .load(
            "dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/acc_status/silver/"
          )
      case "recipes_fabric" =>
        spark.read
          .format("csv")
          .option("header", true)
          .option("sep",    ",")
          .schema(
            StructType(
              Array(
                StructField("acc_id",        IntegerType,   true),
                StructField("person_id",     StringType,    true),
                StructField("product_id",    StringType,    true),
                StructField("business_date", DateType,      true),
                StructField("balance",       DoubleType,    true),
                StructField("import_ts",     TimestampType, false)
              )
            )
          )
          .load(
            "dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/acc_status/silver"
          )
      case _ =>
        throw new Exception("No valid dataset present to read fabric")
    }
  }

}
