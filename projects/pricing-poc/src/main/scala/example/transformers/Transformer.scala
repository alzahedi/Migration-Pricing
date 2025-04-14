package example.transformers

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Transformer {
  def transform(df: DataFrame)(implicit spark: SparkSession): DataFrame
}
