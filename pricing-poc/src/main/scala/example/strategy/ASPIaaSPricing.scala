package example.strategy

import org.apache.spark.sql.DataFrame
class ASPIaaSPricing extends PricingStrategy {
  override def computeCost(platformDf: DataFrame, pricingDf: DataFrame, reservationTerm: String): Double =  ???

  override def storageCost(platformDf: DataFrame, pricingDf: DataFrame): Double = ???
}
