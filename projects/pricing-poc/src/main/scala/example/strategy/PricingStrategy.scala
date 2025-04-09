package example.strategy
import org.apache.spark.sql.{DataFrame,Column}
import org.apache.spark.sql.functions._

trait PricingStrategy {
  def computeCost(platformDf: DataFrame, pricingDf: DataFrame, storageDf: DataFrame): DataFrame
  //def storageCost(platformDf: DataFrame, pricingDf: DataFrame): DataFrame

   protected def calculateMonthlyCost(column: Column, factor: Double, operation: (Column, Double) => Column): Column = {
    round(operation(column, factor), 2)
  }
}
