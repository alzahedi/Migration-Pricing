package example.strategy
import org.apache.spark.sql.DataFrame

trait PricingStrategy {
  def computeCost(platformDf: DataFrame, pricingDf: DataFrame, reservationTerm: String): DataFrame
  def storageCost(platformDf: DataFrame, pricingDf: DataFrame): DataFrame
}
