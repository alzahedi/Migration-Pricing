package example.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkUtils {

  def createSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .appName("SparkAzureSasExample")
      .config("spark.master", "local[*]")
      .getOrCreate()
  }
}
