package example.strategy
import org.apache.spark.sql.{DataFrame,Column}
import org.apache.spark.sql.functions._

trait PricingStrategy {
  def computeCost(platformDf: DataFrame, pricingDf: DataFrame, reservationTerm: String): DataFrame
  def storageCost(platformDf: DataFrame, pricingDf: DataFrame): DataFrame

   protected def calculateMonthlyCost(df: DataFrame, columnName: String, factor: Double, operation: (Column, Double) => Column): DataFrame = {
    df.withColumn(columnName, round(operation(col(columnName), factor), 2))
  }
}
