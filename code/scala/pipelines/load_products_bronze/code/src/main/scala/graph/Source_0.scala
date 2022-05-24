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
        import org.apache.avro.Schema
        var reader = spark.read
          .format("avro")
          .option("ignoreExtension",     false)
          .option("recursiveFileLookup", true)
        reader = reader.option("avroSchema",
                               new Schema.Parser()
                                 .parse("com.databricks.spark.avro".stripMargin)
                                 .toString()
        )
        reader.load(
          "dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/external/products/"
        )
      case _ =>
        throw new Exception("No valid dataset present to read fabric")
    }
  }

}
