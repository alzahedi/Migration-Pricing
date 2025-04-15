package example.transformers

import org.apache.spark.sql.{DataFrame, SparkSession}

trait DataTransformer {
  def transform(df: DataFrame): DataFrame
}
