package example.strategy

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import example.constants.{PricingType, RecommendationConstants, DiskTypeToTierMap}

abstract class BaseIaaSPricing extends PricingStrategy {

  /** Subclasses define their pricing type */
  def pricingType: String

  /** Subclasses define additional filters on the pricing DataFrame */
  def applyAdditionalFilters(df: DataFrame): DataFrame

  /** Subclasses define how to compute cost */
  def deriveComputeCost(joinedDF: DataFrame, reservationTerm: String): DataFrame

  override def computeCost(platformDf: DataFrame, pricingDf: DataFrame, reservationTerm: String): DataFrame = {
    val flattenedDf = platformDf
      .withColumn("SkuRecommendationForServers", explode(col("SkuRecommendationForServers")))
      .withColumn("SkuRecommendationResults", explode(col("SkuRecommendationForServers.SkuRecommendationResults")))
      .select(
        col("SkuRecommendationForServers.ServerName"),
        col("SkuRecommendationResults.TargetSku"))

    val targetSkuExpandedDF = flattenedDf
      .select(col("ServerName"), col("TargetSku.VirtualMachineSize.AzureSkuName").alias("azureSkuName"))

    var filteredPricingDF = pricingDf
      .filter(
        !col("SkuName").contains("Spot") && 
        !col("SkuName").contains("Low Priority") &&
        col("unitOfMeasure") === "1 Hour" &&
        col("location") === "US West" &&
        col("type") === pricingType
      )

    // Apply additional filters specific to each subclass
    filteredPricingDF = applyAdditionalFilters(filteredPricingDF)

    val joinedDF = targetSkuExpandedDF
      .join(filteredPricingDF, col("armSkuName") === col("azureSkuName"), "inner")

    // Call subclass-specific compute logic
    deriveComputeCost(joinedDF, reservationTerm)
  }

  override def storageCost(platformDf: DataFrame, pricingDf: DataFrame): DataFrame = {

    val flattenedDf = platformDf
      .withColumn("SkuRecommendationForServers", explode(col("SkuRecommendationForServers")))
      .withColumn("SkuRecommendationResults", explode(col("SkuRecommendationForServers.SkuRecommendationResults")))
      .select(
        col("SkuRecommendationForServers.ServerName"),
        col("SkuRecommendationResults.TargetSku"))

    val targetSkuExpandedDF = flattenedDf
      .select(col("ServerName"), 
              col("TargetSku.DataDiskSizes").alias("dataDiskSizes"), 
              col("TargetSku.LogDiskSizes").alias("logDiskSizes"),
              col("TargetSku.TempDbDiskSizes").alias("tempDbDiskSizes")        
            )

    // targetSkuExpandedDF.printSchema()
    // targetSkuExpandedDF.show(false)  

    val explodedDF = targetSkuExpandedDF
      .withColumn(
        "allDisks",
        flatten(array(
          coalesce(col("dataDiskSizes"), array()), 
          coalesce(col("logDiskSizes"), array()), 
          coalesce(col("tempDbDiskSizes"), array())
        )) // Flatten ensures we get a single array
      )
      .withColumn("disk", explode(col("allDisks"))) // Explode into individual records
      .select(
        col("disk.Type").alias("DiskType"),
        col("disk.MaxSizeInGib"),
        col("disk.MaxIOPS"),
        col("disk.MaxThroughputInMbps"),
        col("disk.Size")
      )

    // explodedDF.printSchema()
    //explodedDF.show(false)

    val premiumSDV2DiskPrices = getPremiumSSDV2DiskPrices(pricingDf)

    val premiumSSDV2PricesDF = premiumSDV2DiskPrices
      .groupBy()
      .pivot("meterType", Seq("Storage", "IOPS", "Throughput"))
      .agg(first("retailPrice"))
      .withColumnRenamed("Storage", "StoragePrice")
      .withColumnRenamed("IOPS", "IopsPrice")
      .withColumnRenamed("Throughput", "ThroughputPrice")

    // premiumSSDV2PricesDF.printSchema()
    // premiumSSDV2PricesDF.show(false)
  
    val premiumSSDV2DF = explodedDF
      .filter(col("DiskType") === "PremiumSSDV2")
      .join(broadcast(premiumSSDV2PricesDF), lit(true), "left")
      .withColumn("pricePerGib", round(col("StoragePrice") * 24 * 30.5, 4))
      .withColumn("pricePerIOPS", round(col("IopsPrice") * 24 * 30.5, 4))
      .withColumn("pricePerMbps", round(col("ThroughputPrice") * 24 * 30.5, 4))
      .withColumn("StorageCost", col("pricePerGib") * col("MaxSizeInGib"))
      .withColumn("IOPSCost", col("pricePerIOPS") * (col("MaxIOPS") - 3000))
      .withColumn("ThroughputCost", col("pricePerMbps") * (col("MaxThroughputInMbps") - 125))
      .withColumn(
        "monthlyCost",
        when(col("pricePerGib").isNull || col("pricePerIOPS").isNull || col("pricePerMbps").isNull, -1.0)
        .otherwise(col("StorageCost") + col("IOPSCost") + col("ThroughputCost"))
      )
      .agg(sum(col("monthlyCost")).alias("TotalPremiumSSDV2Cost"))
      //.select(col("monthlyCost").alias("TotalPremiumSSDV2Cost"))

      // premiumSSDV2DF.printSchema()
      // premiumSSDV2DF.show(false)

      val targetAlias = "target"
      val pricingAlias = "pricing"
      
      val premiumSSDDF = explodedDF.as(targetAlias)
        .filter(col("DiskType") === "PremiumSSD")
        .join(
          pricingDf.as(pricingAlias)
            .filter(
              col("type") === "Consumption" &&
              col("unitOfMeasure") === "1/Month" &&
              !col("meterName").contains("Free")
            ),
          col(s"$pricingAlias.meterName")
            .equalTo(concat(col(s"$targetAlias.Size").cast("string"), lit(" LRS Disk"))) &&
          col(s"$pricingAlias.productName")
            .contains(DiskTypeToTierMap.map.getOrElse(col(s"$targetAlias.DiskType").toString, "")),
          "left"
        )
        .agg(sum(col(s"$pricingAlias.retailPrice")).alias("TotalPremiumSSDCost"))
       //.select(col(s"$pricingAlias.retailPrice").alias("TotalPremiumSSDCost"))

      // premiumSSDDF.printSchema()
     // premiumSSDDF.show(false)
  
      val otherDisksDF = explodedDF.as(targetAlias)
        .filter(col("DiskType") =!= "PremiumSSDV2" && col("DiskType") =!= "PremiumSSD")
        .join(pricingDf.as(pricingAlias)
          .filter(
            col("type") === "Consumption" &&
            col("unitOfMeasure") === "1/Month" &&
            !col("meterName").contains("Free")
          ),
           concat(col(s"$targetAlias.Size"), lit(" Disks")).equalTo(col(s"$pricingAlias.MeterName")) &&
           col(s"$pricingAlias.productName")
            .contains(DiskTypeToTierMap.map.getOrElse(col(s"$targetAlias.DiskType").toString, "")),
          "left"
        )
        .agg(sum(col(s"$pricingAlias.retailPrice")).alias("TotalOtherDiskCost"))
        //.select(col(s"$pricingAlias.retailPrice").alias("TotalOtherDiskCost"))
      
      val premiumSSDV2DF_fixed = premiumSSDV2DF
        .withColumn("TotalPremiumSSDV2Cost", coalesce(col("TotalPremiumSSDV2Cost"), lit(0)))
        .withColumn("TotalPremiumSSDCost", lit(0)) // Fill missing columns
        .withColumn("TotalOtherDiskCost", lit(0))

      val premiumSSDDF_fixed = premiumSSDDF
        .withColumn("TotalPremiumSSDV2Cost", lit(0))
        .withColumn("TotalPremiumSSDCost", coalesce(col("TotalPremiumSSDCost"), lit(0)))
        .withColumn("TotalOtherDiskCost", lit(0))

      val otherDisksDF_fixed = otherDisksDF
        .withColumn("TotalPremiumSSDV2Cost", lit(0))
        .withColumn("TotalPremiumSSDCost", lit(0))
        .withColumn("TotalOtherDiskCost", coalesce(col("TotalOtherDiskCost"), lit(0)))

      // Union all DataFrames since all columns are now aligned
     val finalDF = premiumSSDV2DF_fixed
      .union(premiumSSDDF_fixed)
      .union(otherDisksDF_fixed)
      .agg(
          sum(col("TotalPremiumSSDV2Cost"))
            .plus(sum(col("TotalPremiumSSDCost")))
            .plus(sum(col("TotalOtherDiskCost")))
            .alias("storageCost") 
      )

      //finalDF.show()
      finalDF
  }

  def getPremiumSSDV2DiskPrices(pricingDF: DataFrame): DataFrame = {
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
    
    premiumSSDV2StorageDF.printSchema()
    premiumSSDV2StorageDF.show(false)

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
}
