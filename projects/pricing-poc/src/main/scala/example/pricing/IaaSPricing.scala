package example.pricing

import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions._
import example.constants.PricingType

object IaaSPricing{
    // Entry point to transform the incoming platform DataFrame
  def transformPlatform(): DataFrame => DataFrame = { df =>
    df
      .withColumn("SkuRecommendationForServers", explode(col("SkuRecommendationForServers")))
      .withColumn("SkuRecommendationResults", explode(col("SkuRecommendationForServers.SkuRecommendationResults")))
      .withColumn("armSkuName", col("SkuRecommendationResults.TargetSku.VirtualMachineSize.AzureSkuName"))
  }

  def enrichWithReservedPricing(pricingDf: DataFrame, storageDf: DataFrame, reservationTerm: String): DataFrame => DataFrame = { platformDf =>
    var minFilteredPricingDF = pricingDf
      .filter(
        !col("skuName").contains("Spot") && 
        !col("skuName").contains("Low Priority") &&
        col("unitOfMeasure") === "1 Hour" &&
        col("location") === "US West" &&
        col("type") === PricingType.Reservation.toString &&
        col("reservationTerm").isNotNull && 
        col("reservationTerm") === reservationTerm
      )
      .groupBy("armSkuName")
      .agg(min("retailPrice").alias(s"minRetailPrice_$reservationTerm"))

    minFilteredPricingDF.filter(col("armSkuName") === "Standard_D2as_v4").select("armSkuName", s"minRetailPrice_$reservationTerm").show(false)
    
    platformDf
      .join(minFilteredPricingDF, Seq("armSkuName"), "inner")
      .withColumn(s"computeCost_${reservationTermToColName(reservationTerm)}",
        computeMonthlyCost(
          col(s"minRetailPrice_$reservationTerm"),
          reservationTermToFactor(reservationTerm)
        )
      )
    
  }

  private def computeMonthlyCost(priceColumn: Column, months: Double): Column = {
      round(priceColumn / months, 2)
  }

  def addMonthlyCostOptions(): DataFrame => DataFrame = { df =>
    df.withColumn("monthlyCostOptions", array(
      struct(
        lit("With1YearRIAndProd").as("keyName"),
        struct(
          col("computeCost_1Yr").as("computeCost"),
          lit(0.0).as("storageCost"),
          //col("storageCost"),
          lit(0.0).as("iopsCost")
        ).as("keyValue")
      ),
      struct(
        lit("With3YearRIAndProd").as("keyName"),
        struct(
          col("computeCost_3Yr").as("computeCost"),
          lit(0.0).as("storageCost"),
          //col("storageCost"),
          lit(0.0).as("iopsCost")
        ).as("keyValue")
      )
    ))
  }

  private val reservationTermToColName: Map[String, String] = Map(
    "1 Year" -> "1Yr",
    "3 Years" -> "3Yr"
  )

   private val reservationTermToFactor: Map[String, Double] = Map(
    "1 Year" -> 12.0,
    "3 Years" -> 36.0
  )
}
