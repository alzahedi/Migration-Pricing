package example.strategy

import org.apache.spark.sql.DataFrame
class ASPIaaSPricing extends PricingStrategy {
  override def computeCost(platformDf: DataFrame, pricingDf: DataFrame, reservationTerm: String): DataFrame =  ???

  override def storageCost(platformDf: DataFrame, pricingDf: DataFrame): DataFrame = ???
}
