package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._

object Report {

  def apply(spark: SparkSession, in: DataFrame): Unit = {
    Config.fabricName match {
      case "anshuman2" =>
        in.write
          .format("delta")
          .option("fileFormat", "parquet")
          .mode("overwrite")
          .option(
            "path",
            "dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/gold/".trim
          )
          .partitionBy("business_date")
          .saveAsTable("anshu.fin_reporting_gold")
      case _ =>
        throw new Exception("No valid dataset present to read fabric")
    }
  }

}
