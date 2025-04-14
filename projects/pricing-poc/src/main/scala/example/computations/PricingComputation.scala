package example.computations

import org.apache.spark.sql.{DataFrame, SparkSession}

trait PricingComputation {
  def compute(df: DataFrame)(implicit spark: SparkSession): DataFrame
}
