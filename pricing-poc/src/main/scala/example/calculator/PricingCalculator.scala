package example.calculator
import org.apache.spark.sql.DataFrame


trait PricingCalculator {
    def calculateReservedComputeCost(platformDf: DataFrame, pricingDf: DataFrame, reservationTerm: String): Double

    def calculateReservedStorageCost(platformDf: DataFrame, pricingDf: DataFrame): Double

    def calculateDevTestReservedComputeCost(platformDf: DataFrame, pricingDf: DataFrame, reservationTerm: String): Double

    def calculateDevTestReservedStorageCost(platformDf: DataFrame, pricingDf: DataFrame): Double
}
