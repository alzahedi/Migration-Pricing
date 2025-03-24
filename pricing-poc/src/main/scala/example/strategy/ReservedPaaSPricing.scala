package example.strategy

import org.apache.spark.sql.DataFrame
import example.calculator.PaasPricingCalculator

class ReservedPaaSPricing extends PricingStrategy {
  override def computeCost(platformDf: DataFrame, pricingDf: DataFrame, reservationTerm: String): Double = {
    val calculator = new PaasPricingCalculator
    calculator.calculateReservedComputeCost(platformDf, pricingDf, reservationTerm)
  }

  override def storageCost(platformDf: DataFrame, pricingDf: DataFrame): Double = {
    val calculator = new PaasPricingCalculator
    calculator.calculateReservedStorageCost(platformDf, pricingDf)
  }
}

