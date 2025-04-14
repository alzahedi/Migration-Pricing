package example.pricing

import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions._
import example.constants.{PricingType, RecommendationConstants, DiskTypeToTierMap}

object IaaSPricing {

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
    val filteredPricing = pricingDf
      .filter(basePricingFilter("Reservation", "US West") &&
        col("reservationTerm") === reservationTerm
      )
      .groupBy("armSkuName")
      .agg(min("retailPrice").alias(s"minRetailPrice_$reservationTerm"))

    platformDf
      .join(filteredPricing, Seq("armSkuName"), "left")
      .withColumn(s"computeCostRI_${reservationTermToColName(reservationTerm)}",
        calculateMonthlyCost(
          coalesce(col(s"minRetailPrice_$reservationTerm"), lit(0.0)),
          reservationTermToFactor(reservationTerm),
          _ / _
        )
      )
  }

  def enrichWithAspPricing(pricingDf: DataFrame, reservationTerm: String, pricingType: PricingType, label: String): DataFrame => DataFrame = { platformDf =>
    val filteredPricing = pricingDf
      .filter(basePricingFilter(pricingType.toString, "US West") &&
        col("savingsPlan").isNotNull
      )
      .withColumn("savingsPlan", explode(col("savingsPlan")))
      .filter(col("savingsPlan.term") === lit(reservationTerm))
      .groupBy("armSkuName")
      .agg(min("savingsPlan.retailPrice").alias(s"minRetailPrice_${label}_$reservationTerm"))

    platformDf
      .join(filteredPricing, Seq("armSkuName"), "left")
      .withColumn(s"computeCost${label}_${reservationTermToColName(reservationTerm)}",
        calculateMonthlyCost(
          coalesce(col(s"minRetailPrice_${label}_$reservationTerm"), lit(0.0)),
          24 * 30.5,
          _ * _
        )
      )
  }

  def enrichWithAspProdPricing(pricingDf: DataFrame, reservationTerm: String): DataFrame => DataFrame =
    enrichWithAspPricing(pricingDf, reservationTerm, PricingType.Consumption, "ASPProd")

  def enrichWithAspDevTestPricing(pricingDf: DataFrame, reservationTerm: String): DataFrame => DataFrame =
    enrichWithAspPricing(pricingDf, reservationTerm, PricingType.DevTestConsumption, "ASPDevTest")

  def enrichWithStoragePricing(storagePricingDF: DataFrame): DataFrame => DataFrame = { platformDf =>
    val premiumSSDV2PricesDF = getPremiumSSDV2DiskPrices(storagePricingDF)
      .groupBy()
      .pivot("meterType", Seq("Storage", "IOPS", "Throughput"))
      .agg(first("retailPrice"))
      .withColumnRenamed("Storage", "StoragePrice")
      .withColumnRenamed("IOPS", "IopsPrice")
      .withColumnRenamed("Throughput", "ThroughputPrice")

    val enrichedWithSSDV2 = platformDf
      .withColumn("DiskType", col("disk.Type"))
      .join(broadcast(premiumSSDV2PricesDF), lit(true), "left")
      .withColumn("pricePerGib", round(col("StoragePrice") * 24 * 30.5, 4))
      .withColumn("pricePerIOPS", round(col("IopsPrice") * 24 * 30.5, 4))
      .withColumn("pricePerMbps", round(col("ThroughputPrice") * 24 * 30.5, 4))
      .withColumn("premiumSSDV2StorageCost", when(col("DiskType") === "PremiumSSDV2", col("pricePerGib") * col("disk.MaxSizeInGib")))
      .withColumn("premiumSSDV2IOPSCost", when(col("DiskType") === "PremiumSSDV2", col("pricePerIOPS") * (col("disk.MaxIOPS") - 3000)))
      .withColumn("premiumSSDV2ThroughputCost", when(col("DiskType") === "PremiumSSDV2", col("pricePerMbps") * (col("disk.MaxThroughputInMbps") - 125)))
      .withColumn("TotalPremiumSSDV2Cost", when(col("DiskType") === "PremiumSSDV2", 
        col("premiumSSDV2StorageCost") + col("premiumSSDV2IOPSCost") + col("premiumSSDV2ThroughputCost"))
        .otherwise(0.0)
      )

    val diskPricingFiltered = storagePricingDF
      .filter(col("type") === "Consumption" && col("unitOfMeasure") === "1/Month" && !col("meterName").contains("Free"))

    enrichedWithSSDV2
      .join(diskPricingFiltered,
        when(col("DiskType") === "PremiumSSD",
          col("meterName") === concat(col("disk.Size"), lit(" LRS Disk")) &&
          col("productName").contains(DiskTypeToTierMap.map.getOrElse(col("DiskType").toString, ""))
        ).otherwise(
          concat(col("disk.Size"), lit(" Disks")) === col("meterName") &&
          col("productName").contains(DiskTypeToTierMap.map.getOrElse(col("DiskType").toString, ""))
        ),
        "left"
      )
      .withColumn("TotalPremiumSSDCost",
        when(col("DiskType") === "PremiumSSD", coalesce(col("retailPrice"), lit(0))).otherwise(0.0)
      )
      .withColumn("TotalOtherDiskCost",
        when(col("DiskType") =!= "PremiumSSDV2" && col("DiskType") =!= "PremiumSSD", coalesce(col("retailPrice"), lit(0))).otherwise(0.0)
      )
      .withColumn("storageCost", col("TotalPremiumSSDV2Cost") + col("TotalPremiumSSDCost") + col("TotalOtherDiskCost"))
  }

  def addMonthlyCostOptions(): DataFrame => DataFrame = { df =>
    df.withColumn("monthlyCostOptions", array(
      buildMonthlyCostStruct("With1YearASPAndDevTest", "computeCostASPDevTest_1Year"),
      buildMonthlyCostStruct("With3YearASPAndDevTest", "computeCostASPDevTest_3Years"),
      buildMonthlyCostStruct("With1YearASPAndProd", "computeCostASPProd_1Year"),
      buildMonthlyCostStruct("With3YearASPAndProd", "computeCostASPProd_3Years"),
      buildMonthlyCostStruct("With1YearRIAndProd", "computeCostRI_1Year"),
      buildMonthlyCostStruct("With3YearRIAndProd", "computeCostRI_3Years"),
    ))
  }

  // Helpers
  private def reservationTermToColName(term: String): String = term.replaceAll("[^A-Za-z0-9]", "")
  private def reservationTermToFactor(term: String): Double = term match {
    case "1 Yr" => 12.0
    case "3 Yr" => 36.0
    case _      => 12.0
  }

  private def calculateMonthlyCost(column: Column, factor: Double, op: (Column, Double) => Column): Column =
    round(op(column, factor), 2)

  private def computeMonthlyCost(priceColumn: Column, months: Double): Column =
    round(priceColumn / months, 2)

  private def buildMonthlyCostStruct(label: String, computeCol: String): Column =
    struct(
      lit(label).as("keyName"),
      struct(
        col(computeCol).as("computeCost"),
        col("storageCost"),
        lit(0.0).as("iopsCost")
      ).as("keyValue")
    )

  private def getPremiumSSDV2DiskPrices(pricingDF: DataFrame): DataFrame = {
    def getMeterDF(meterName: String, meterType: String): DataFrame =
      pricingDF
        .filter(
          col("meterName") === meterName &&
          col("productName").contains(RecommendationConstants.PremiumSSDV2ProductName) &&
          col("type") === "Consumption" &&
          lower(col("unitOfMeasure")) === "1 gib/hour" &&
          col("retailPrice") > 0
        )
        .orderBy("retailPrice")
        .limit(1)
        .withColumn("meterType", lit(meterType))

    getMeterDF(RecommendationConstants.PremiumSSDV2StorageMeterName, "Storage")
      .union(getMeterDF(RecommendationConstants.PremiumSSDV2IOPSMeterName, "IOPS"))
      .union(getMeterDF(RecommendationConstants.PremiumSSDV2ThroughputMeterName, "Throughput"))
      .select("meterType", "retailPrice")
  }

  private def basePricingFilter(pricingType: String, location: String): Column = {
    !col("skuName").contains("Spot") &&
    !col("skuName").contains("Low Priority") &&
    col("unitOfMeasure") === "1 Hour" &&
    col("location") === location &&
    col("type") === pricingType
  }
}
