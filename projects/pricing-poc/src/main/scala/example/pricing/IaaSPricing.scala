package example.pricing

import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions._
import example.constants.{PricingType, RecommendationConstants}
import example.constants.DiskTypeToTierMap

object IaaSPricing{
    // Entry point to transform the incoming platform DataFrame
  def transformPlatform(): DataFrame => DataFrame = { df =>
    df
      .withColumn("SkuRecommendationForServers", explode(col("SkuRecommendationForServers")))
      .withColumn("SkuRecommendationResults", explode(col("SkuRecommendationForServers.SkuRecommendationResults")))
      .withColumn("armSkuName", col("SkuRecommendationResults.TargetSku.VirtualMachineSize.AzureSkuName"))
      .withColumn(
        "allDisks",
        flatten(array(
          coalesce(col("SkuRecommendationResults.TargetSku.DataDiskSizes"), array()),
          coalesce(col("SkuRecommendationResults.TargetSku.LogDiskSizes"), array()),
          coalesce(col("SkuRecommendationResults.TargetSku.TempDbDiskSizes"), array())
        ))
      )
      .withColumn("disk", explode(col("allDisks")))
    }

  def enrichWithReservedPricing(pricingDf: DataFrame, reservationTerm: String): DataFrame => DataFrame = { platformDf =>
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
        
    platformDf
      .join(minFilteredPricingDF, Seq("armSkuName"), "left")
      .withColumn(s"computeCostRI_${reservationTermToColName(reservationTerm)}",
        calculateMonthlyCost(
          coalesce(col(s"minRetailPrice_$reservationTerm"), lit(0.0)),
          reservationTermToFactor(reservationTerm),
          _ / _
        )
      )
  }

  def enrichWithAspProdPricing(pricingDf: DataFrame, reservationTerm: String): DataFrame => DataFrame = { platformDf => 
    var minFilteredPricingDF = pricingDf
    .filter(
      !col("SkuName").contains("Spot") &&
      !col("SkuName").contains("Low Priority") &&
      col("unitOfMeasure") === "1 Hour" &&
      col("location") === "US West" &&
      col("type") === PricingType.Consumption.toString &&
      col("savingsPlan").isNotNull
    )
    .withColumn("savingsPlan", explode(col("savingsPlan")))
    .filter(col("savingsPlan.term") === lit(reservationTerm))
    .groupBy("armSkuName")
    .agg(min("savingsPlan.retailPrice").alias(s"minRetailPriceAspProd_$reservationTerm"))

    platformDf
    .join(minFilteredPricingDF, Seq("armSkuName"), "left")
    .withColumn(s"computeCostASPProd_${reservationTermToColName(reservationTerm)}",
      calculateMonthlyCost(
        coalesce(col(s"minRetailPriceAspProd_$reservationTerm"), lit(0.0)),
         24 * 30.5,
         _ * _
      )
    )
  }

  def enrichWithAspDevTestPricing(pricingDf: DataFrame, reservationTerm: String): DataFrame => DataFrame = { platformDf => 
    var minFilteredPricingDF = pricingDf
    .filter(
      !col("SkuName").contains("Spot") &&
      !col("SkuName").contains("Low Priority") &&
      col("unitOfMeasure") === "1 Hour" &&
      col("location") === "US West" &&
      col("type") === PricingType.DevTestConsumption.toString &&
      col("savingsPlan").isNotNull
    )
    .withColumn("savingsPlan", explode(col("savingsPlan")))
    .filter(col("savingsPlan.term") === lit(reservationTerm))
    .groupBy("armSkuName")
    .agg(min("savingsPlan.retailPrice").alias(s"minRetailPriceAspDevTest_$reservationTerm"))

    platformDf
    .join(minFilteredPricingDF, Seq("armSkuName"), "left")
    .withColumn(s"computeCostASPDevTest_${reservationTermToColName(reservationTerm)}",
      calculateMonthlyCost(
        coalesce(col(s"minRetailPriceAspDevTest_$reservationTerm"), lit(0.0)),
         24 * 30.5,
         _ * _
      )
    )
  }

  def enrichWithStoragePricing(storagePricingDF: DataFrame): DataFrame => DataFrame = { platformDf => 
    val premiumSDV2DiskPrices = getPremiumSSDV2DiskPrices(storagePricingDF)

    val premiumSSDV2PricesDF = premiumSDV2DiskPrices
      .groupBy()
      .pivot("meterType", Seq("Storage", "IOPS", "Throughput"))
      .agg(first("retailPrice"))
      .withColumnRenamed("Storage", "StoragePrice")
      .withColumnRenamed("IOPS", "IopsPrice")
      .withColumnRenamed("Throughput", "ThroughputPrice")
    
    premiumSSDV2PricesDF.show(false)

    val enrichedDF = platformDf
      .withColumn("DiskType", col("disk.Type"))
      .join(broadcast(premiumSSDV2PricesDF), lit(true), "left")
      .withColumn("pricePerGib", round(col("StoragePrice") * 24 * 30.5, 4))
      .withColumn("pricePerIOPS", round(col("IopsPrice") * 24 * 30.5, 4))
      .withColumn("pricePerMbps", round(col("ThroughputPrice") * 24 * 30.5, 4))
      .withColumn("premiumSSDV2StorageCost", 
        when(col("DiskType") === "PremiumSSDV2", col("pricePerGib") * col("disk.MaxSizeInGib"))
      )
      .withColumn("premiumSSDV2IOPSCost", 
        when(col("DiskType") === "PremiumSSDV2", col("pricePerIOPS") * (col("disk.MaxIOPS") - 3000))
      )
      .withColumn("premiumSSDV2ThroughputCost", 
        when(col("DiskType") === "PremiumSSDV2", col("pricePerMbps") * (col("disk.MaxThroughputInMbps") - 125))
      )
      .withColumn("TotalPremiumSSDV2Cost",
        when(
          col("DiskType") === "PremiumSSDV2" &&
          col("pricePerGib").isNotNull &&
          col("pricePerIOPS").isNotNull &&
          col("pricePerMbps").isNotNull,
          col("premiumSSDV2StorageCost") + col("premiumSSDV2IOPSCost") + col("premiumSSDV2ThroughputCost")
        ).otherwise(0.0)
      )

    enrichedDF
      .join(
        storagePricingDF
          .filter(
            col("type") === "Consumption" &&
            col("unitOfMeasure") === "1/Month" &&
            !col("meterName").contains("Free")
          ),
        when(col("DiskType") === "PremiumSSD",
          col("meterName") === concat(col("disk.Size"), lit(" LRS Disk")) &&
          col("productName").contains(
            DiskTypeToTierMap.map.getOrElse(col("DiskType").toString, "")
          )
        ).otherwise(
          concat(col("disk.Size"), lit(" Disks")) === col("meterName") &&
          col("productName").contains(
            DiskTypeToTierMap.map.getOrElse(col("DiskType").toString, "")
          )
        ),
        "left"
      )
      .withColumn("TotalPremiumSSDCost",
        when(col("DiskType") === "PremiumSSD", coalesce(col("retailPrice"), lit(0)))
        .otherwise(0.0)
      )
      .withColumn("TotalOtherDiskCost",
        when(col("DiskType") =!= "PremiumSSDV2" && col("DiskType") =!= "PremiumSSD",
          coalesce(col("retailPrice"), lit(0))
        ).otherwise(0.0)
      )
      .withColumn("storageCost", col("TotalPremiumSSDV2Cost") + col("TotalPremiumSSDCost") + col("TotalOtherDiskCost"))
  }

  private def computeMonthlyCost(priceColumn: Column, months: Double): Column = {
      round(priceColumn / months, 2)
  }

   def calculateMonthlyCost(column: Column, factor: Double, operation: (Column, Double) => Column): Column = {
    round(operation(column, factor), 2)
  }

  private def getPremiumSSDV2DiskPrices(pricingDF: DataFrame): DataFrame = {
    val premiumSSDV2StorageDF = pricingDF
      .filter(
        col("meterName") === RecommendationConstants.PremiumSSDV2StorageMeterName &&
        col("productName").contains(RecommendationConstants.PremiumSSDV2ProductName) &&
        col("type") === "Consumption" &&
        lower(col("unitOfMeasure")) === "1 gib/hour" &&
        col("retailPrice") > 0
      )
      .orderBy("RetailPrice")
      .limit(1)
      .withColumn("meterType", lit("Storage"))

    val premiumSSDV2IOPSDF = pricingDF
      .filter(
        col("meterName") === RecommendationConstants.PremiumSSDV2IOPSMeterName &&
        col("productName").contains(RecommendationConstants.PremiumSSDV2ProductName) &&
        col("type") === "Consumption" &&
        col("unitOfMeasure") === RecommendationConstants.MeterUnitPerHour &&
        col("retailPrice") > 0
      )
      .orderBy("retailPrice")
      .limit(1)
      .withColumn("meterType", lit("IOPS"))

    val premiumSSDV2ThroughputDF = pricingDF
      .filter(
        col("meterName") === RecommendationConstants.PremiumSSDV2ThroughputMeterName &&
        col("productName").contains(RecommendationConstants.PremiumSSDV2ProductName) &&
        col("type") === "Consumption" &&
        col("unitOfMeasure") === RecommendationConstants.MeterUnitPerHour &&
        col("retailPrice") > 0
      )
      .orderBy("retailPrice")
      .limit(1)
      .withColumn("meterType", lit("Throughput"))

    // Union all three data frames
    premiumSSDV2StorageDF
      .union(premiumSSDV2IOPSDF)
      .union(premiumSSDV2ThroughputDF)
      .select("meterType", "retailPrice")
  }

  def addMonthlyCostOptions(): DataFrame => DataFrame = { df =>
    df.withColumn("monthlyCostOptions", array(
      struct(
        lit("With1YearASPAndDevTest").as("keyName"),
        struct(
          col("computeCostASPDevTest_1Yr").as("computeCost"),
          col("storageCost"),
          lit(0.0).as("iopsCost")
        ).as("keyValue")
      ),
      struct(
        lit("With3YearASPAndDevTest").as("keyName"),
        struct(
          col("computeCostASPDevTest_3Yr").as("computeCost"),
          col("storageCost"),
          lit(0.0).as("iopsCost")
        ).as("keyValue")
      ),
      struct(
        lit("With1YearASPAndProd").as("keyName"),
        struct(
          col("computeCostASPProd_1Yr").as("computeCost"),
          col("storageCost"),
          lit(0.0).as("iopsCost")
        ).as("keyValue")
      ),
      struct(
        lit("With3YearASPAndProd").as("keyName"),
        struct(
          col("computeCostASPProd_3Yr").as("computeCost"),
          col("storageCost"),
          lit(0.0).as("iopsCost")
        ).as("keyValue")
      ),
      struct(
        lit("With1YearRIAndProd").as("keyName"),
        struct(
          col("computeCostRI_1Yr").as("computeCost"),
          col("storageCost"),
          lit(0.0).as("iopsCost")
        ).as("keyValue")
      ),
      struct(
        lit("With3YearRIAndProd").as("keyName"),
        struct(
          col("computeCostRI_3Yr").as("computeCost"),
          col("storageCost"),
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
