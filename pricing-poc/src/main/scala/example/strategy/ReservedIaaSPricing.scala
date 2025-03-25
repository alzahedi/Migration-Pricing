package example.strategy


import org.apache.spark.sql.DataFrame
import example.calculator.IaasPricingCalculator

class ReservedIaaSPricing extends PricingStrategy {
  override def computeCost(platformDf: DataFrame, pricingDf: DataFrame, reservationTerm: String): Double = {
    val calculator = new IaasPricingCalculator
    calculator.calculateReservedComputeCost(platformDf, pricingDf, reservationTerm)
  }

  override def storageCost(platformDf: DataFrame, pricingDf: DataFrame): Double = {
    0.18
  }
}

