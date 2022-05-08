package graph

import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import config.ConfigStore._
import udfs.UDFs._
import udfs._

object dvr {
  def apply(spark: SparkSession, in0: DataFrame): DataFrame = {
    if (in0.filter(col("tran_amount") < 0).count() > 0){
        throw new RuntimeException("Found negative account balances")
    }else if (in0.filter(!col("tran_type").isin("INTEREST", "DEBIT", "CREDIT")).count() >0){
            throw new RuntimeException("Found invalid Transaction types. Allowed values = [\"INTEREST\", \"DEBIT\", \"CREDIT\"]")
    
    }
    val out0 = in0
        
    out0
  }

}
