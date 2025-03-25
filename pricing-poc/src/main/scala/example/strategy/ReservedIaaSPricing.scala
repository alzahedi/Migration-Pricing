package example.strategy


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import example.constants.PricingType

class ReservedIaaSPricing extends PricingStrategy {
  override def computeCost(platformDf: DataFrame, pricingDf: DataFrame, reservationTerm: String): Double = {
    val flattenedDf = platformDf
      .withColumn("SkuRecommendationForServers", explode(col("SkuRecommendationForServers")))
      .withColumn("SkuRecommendationResults", explode(col("SkuRecommendationForServers.SkuRecommendationResults")))
      .select(
        col("SkuRecommendationForServers.ServerName"),
        col("SkuRecommendationResults.TargetSku"))

    println("Vm Schema")
    flattenedDf.printSchema()
    flattenedDf.show(false)

    val armSkuNameOpt: Option[String] = flattenedDf
      .select(col("TargetSku.VirtualMachineSize.AzureSkuName"))
      .collect()
      .headOption
      .map(_.getString(0))

    val armSkuName = armSkuNameOpt.map(_.trim).getOrElse("")

    val filteredDf = pricingDf.filter(
        col("armSkuName") === armSkuName &&
        !col("SkuName").contains("Spot") && 
        !col("SkuName").contains("Low Priority") &&
        col("unitOfMeasure") === "1 Hour" &&
        col("location") === "US West" &&
        col("type") === PricingType.Reservation.toString &&
        col("reservationTerm") === reservationTerm
    )

    val minPrice = filteredDf.orderBy("retailPrice").select("retailPrice").first().getDouble(0)
    minPrice
  }

  override def storageCost(platformDf: DataFrame, pricingDf: DataFrame): Double = {
    0.18
  }
}

