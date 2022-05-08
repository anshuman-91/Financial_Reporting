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
        import org.apache.avro.Schema
        var reader = spark.read.format("avro")
        reader = reader
        reader = reader.schema(
          StructType(
            Array(
              StructField("id",   IntegerType, false),
              StructField("name", StringType,  false),
              StructField(
                "properties",
                StructType(
                  Array(StructField("bonus_rate",     DoubleType,  false),
                        StructField("lock_in_period", IntegerType, true)
                  )
                ),
                false
              ),
              StructField("slug",       StringType,    false),
              StructField("updated_at", TimestampType, false)
            )
          )
        )
        reader.load(
          "dbfs:/Prophecy/anshuman@simpledatalabs.com/fin_reporting/products/bronze"
        )
      case _ =>
        throw new Exception("No valid dataset present to read fabric")
    }
  }

}
