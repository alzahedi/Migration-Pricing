package example.computations

import org.apache.spark.sql.{DataFrame, SparkSession}

trait PricingComputation {
  def compute(df: DataFrame, computeDF: DataFrame, storageDF: DataFrame): DataFrame
}
