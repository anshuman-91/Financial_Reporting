package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object validate_email {
  def apply(spark: SparkSession, in0: DataFrame): DataFrame = {
    if(in0.withColumn("is_valid_email",
        when(col("email").rlike("^(.+@.+\\.com)(; .+@.+\\.com)*$"),"valid")
        .otherwise("invalid"))
        .filter(col("is_valid_email") === "invalid").count() > 0){
            throw new RuntimeException("Found invalid email addresses")
        }
    val out0 = in0
        
    out0
  }

}
