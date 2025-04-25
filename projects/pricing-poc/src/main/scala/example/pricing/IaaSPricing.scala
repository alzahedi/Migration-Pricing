package example.pricing

import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions._
import example.constants.{PricingType, RecommendationConstants, DiskTypeToTierMap}

object IaaSPricing {

  // def transformPlatform(): DataFrame => DataFrame = { df =>
  //   df
  //     .withColumn("SkuRecommendationForServers", explode(col("SkuRecommendationForServers")))
  //     .withColumn("SkuRecommendationResults", explode(col("SkuRecommendationForServers.SkuRecommendationResults")))
  //     .withColumn("armSkuName", col("SkuRecommendationResults.TargetSku.VirtualMachineSize.AzureSkuName"))
  //     .withColumn(
  //       "allDisks",
  //       flatten(array(
  //         coalesce(col("SkuRecommendationResults.TargetSku.DataDiskSizes"), array()),
  //         coalesce(col("SkuRecommendationResults.TargetSku.LogDiskSizes"), array()),
  //         coalesce(col("SkuRecommendationResults.TargetSku.TempDbDiskSizes"), array())
  //       ))
  //     )
  //     .withColumn("disk", explode(col("allDisks")))
  // }

  def transformPlatform(): DataFrame => DataFrame = { df =>
    df.withColumn(
      "SkuRecommendationForServers",
      transform(col("SkuRecommendationForServers"), server =>
        server.withField(
          "SkuRecommendationResults",
          transform(server.getField("SkuRecommendationResults"), result =>
            result
              .withField("armSkuName", result.getField("TargetSku").getField("VirtualMachineSize").getField("AzureSkuName"))
              .withField("disks",
                flatten(array(
                  coalesce(result.getField("TargetSku").getField("DataDiskSizes"), array()),
                  coalesce(result.getField("TargetSku").getField("LogDiskSizes"), array()),
                  coalesce(result.getField("TargetSku").getField("TempDbDiskSizes"), array())
                )
              )
            )
          )
        )
      )
    )
  }

  // def enrichWithReservedPricing(pricingDf: DataFrame, reservationTerm: String): DataFrame => DataFrame = { platformDf =>
  //   val filteredPricing = pricingDf
  //     .filter(basePricingFilter("Reservation", "US West") &&
  //       col("reservationTerm") === reservationTerm
  //     )
  //     .groupBy("armSkuName")
  //     .agg(min("retailPrice").alias(s"minRetailPrice_$reservationTerm"))

  //   platformDf
  //     .join(filteredPricing, Seq("armSkuName"), "left")
  //     .withColumn(s"computeCostRI_${reservationTermToColName(reservationTerm)}",
  //       calculateMonthlyCost(
  //         coalesce(col(s"minRetailPrice_$reservationTerm"), lit(0.0)),
  //         reservationTermToFactor(reservationTerm),
  //         _ / _
  //       )
  //     )
  // }

  def enrichWithReservedPricing(pricingDf: DataFrame, reservationTerm: String): DataFrame => DataFrame = { platformDf =>
    val minPricingDf = pricingDf
      .filter(basePricingFilter("Reservation", "US West") &&
        col("reservationTerm") === reservationTerm
      )
      .groupBy("armSkuName")
      .agg(min("retailPrice").alias(s"minRetailPrice_$reservationTerm"))

    val pricingMapExprEntries = minPricingDf
      .select(
        col("armSkuName").as("key"),
        col(s"minRetailPrice_$reservationTerm").as("value")
      )
      .collect()
      .flatMap { row =>
        Seq(lit(row.getString(0)), lit(row.getDouble(1)))
      }
    
    val pricingExpr = map(pricingMapExprEntries: _*)

    platformDf.withColumn(
      "SkuRecommendationForServers",
      transform(col("SkuRecommendationForServers"), server =>
        server.withField(
          "SkuRecommendationResults",
          transform(server.getField("SkuRecommendationResults"), result =>
            result.withField(
                s"computeCostRI_${reservationTermToColName(reservationTerm)}",
                calculateMonthlyCost(
                  coalesce(
                    element_at(
                      pricingExpr,
                      concat_ws("|", result.getField("armSkuName"))
                    ), 
                  lit(0.0)),
                reservationTermToFactor(reservationTerm),
                _ / _
                )
            )
          )
        )
      ))
  }

  // d
  def enrichWithAspPricing(pricingDf: DataFrame, reservationTerm: String, pricingType: PricingType, label: String): DataFrame => DataFrame = { platformDf =>
    val filteredPricing = pricingDf
      .filter(basePricingFilter(pricingType.toString, "US West") &&
        col("savingsPlan").isNotNull
      )
      .withColumn("savingsPlan", explode(col("savingsPlan")))
      .filter(col("savingsPlan.term") === lit(reservationTerm))
      .groupBy("armSkuName")
      .agg(min("savingsPlan.retailPrice").alias(s"minRetailPrice_${label}_$reservationTerm"))

  val pricingMapExprEntries = filteredPricing
    .select(
      col("armSkuName").as("key"),
      col(s"minRetailPrice_${label}_$reservationTerm").as("value")
    )
    .collect()
    .flatMap { row =>
      Seq(lit(row.getString(0)), lit(row.getDouble(1)))
    }

  val pricingExpr = map(pricingMapExprEntries: _*)

  platformDf.withColumn(
    "SkuRecommendationForServers",
    transform(col("SkuRecommendationForServers"), server =>
      server.withField(
        "SkuRecommendationResults",
        transform(server.getField("SkuRecommendationResults"), result =>
          result .withField(
              s"computeCost${label}_${reservationTermToColName(reservationTerm)}",
              calculateMonthlyCost(
                  coalesce(
                    element_at(
                      pricingExpr,
                      result.getField("armSkuName")
                    ),
                    lit(0.0)
                  ),
                 24 * 30.5,
                 _ * _
              )
          )
        )
      )
    ))
  }

  def enrichWithAspProdPricing(pricingDf: DataFrame, reservationTerm: String): DataFrame => DataFrame =
    enrichWithAspPricing(pricingDf, reservationTerm, PricingType.Consumption, "ASPProd")

  def enrichWithAspDevTestPricing(pricingDf: DataFrame, reservationTerm: String): DataFrame => DataFrame =
    enrichWithAspPricing(pricingDf, reservationTerm, PricingType.DevTestConsumption, "ASPDevTest")

  // def enrichWithStoragePricing(storagePricingDF: DataFrame): DataFrame => DataFrame = { platformDf =>
  //   val premiumSSDV2PricesDF = getPremiumSSDV2DiskPrices(storagePricingDF)
  //     .groupBy()
  //     .pivot("meterType", Seq("Storage", "IOPS", "Throughput"))
  //     .agg(first("retailPrice"))
  //     .withColumnRenamed("Storage", "StoragePrice")
  //     .withColumnRenamed("IOPS", "IopsPrice")
  //     .withColumnRenamed("Throughput", "ThroughputPrice")

  //   val enrichedWithSSDV2 = platformDf
  //     .withColumn("DiskType", col("disk.Type"))
  //     .join(broadcast(premiumSSDV2PricesDF), lit(true), "left")
  //     .withColumn("pricePerGib", round(col("StoragePrice") * 24 * 30.5, 4))
  //     .withColumn("pricePerIOPS", round(col("IopsPrice") * 24 * 30.5, 4))
  //     .withColumn("pricePerMbps", round(col("ThroughputPrice") * 24 * 30.5, 4))
  //     .withColumn("premiumSSDV2StorageCost", when(col("DiskType") === "PremiumSSDV2", col("pricePerGib") * col("disk.MaxSizeInGib")))
  //     .withColumn("premiumSSDV2IOPSCost", when(col("DiskType") === "PremiumSSDV2", col("pricePerIOPS") * (col("disk.MaxIOPS") - 3000)))
  //     .withColumn("premiumSSDV2ThroughputCost", when(col("DiskType") === "PremiumSSDV2", col("pricePerMbps") * (col("disk.MaxThroughputInMbps") - 125)))
  //     .withColumn("TotalPremiumSSDV2Cost", when(col("DiskType") === "PremiumSSDV2", 
  //       col("premiumSSDV2StorageCost") + col("premiumSSDV2IOPSCost") + col("premiumSSDV2ThroughputCost"))
  //       .otherwise(0.0)
  //     )

  //   val diskPricingFiltered = storagePricingDF
  //     .filter(col("type") === "Consumption" && col("unitOfMeasure") === "1/Month" && !col("meterName").contains("Free"))

  //   enrichedWithSSDV2
  //     .join(diskPricingFiltered,
  //       when(col("DiskType") === "PremiumSSD",
  //         col("meterName") === concat(col("disk.Size"), lit(" LRS Disk")) &&
  //         col("productName").contains(DiskTypeToTierMap.map.getOrElse(col("DiskType").toString, ""))
  //       ).otherwise(
  //         concat(col("disk.Size"), lit(" Disks")) === col("meterName") &&
  //         col("productName").contains(DiskTypeToTierMap.map.getOrElse(col("DiskType").toString, ""))
  //       ),
  //       "left"
  //     )
  //     .withColumn("TotalPremiumSSDCost",
  //       when(col("DiskType") === "PremiumSSD", coalesce(col("retailPrice"), lit(0))).otherwise(0.0)
  //     )
  //     .withColumn("TotalOtherDiskCost",
  //       when(col("DiskType") =!= "PremiumSSDV2" && col("DiskType") =!= "PremiumSSD", coalesce(col("retailPrice"), lit(0))).otherwise(0.0)
  //     )
  //     .withColumn("storageCost", col("TotalPremiumSSDV2Cost") + col("TotalPremiumSSDCost") + col("TotalOtherDiskCost"))
  // }

    def enrichWithStoragePricing(storagePricingDF: DataFrame): DataFrame => DataFrame = { platformDf =>

      val premiumSSDV2PricesDF = getPremiumSSDV2DiskPrices(storagePricingDF) 

      val pivotedDf = premiumSSDV2PricesDF
        .groupBy() 
        .pivot("meterType", Seq("StoragePrice", "IOPSPrice", "ThroughputPrice"))
        .agg(first("retailPrice"))
          
      val enrichedDf = pivotedDf
        .withColumn("pricePerGib", round(col("StoragePrice") * 24 * 30.5, 4))
        .withColumn("pricePerIOPS", round(col("IOPSPrice") * 24 * 30.5, 4))
        .withColumn("pricePerMbps", round(col("ThroughputPrice") * 24 * 30.5, 4))


      val row = enrichedDf.first()

      val priceMap = Map(
        "StoragePrice"   -> row.getAs[Double]("StoragePrice"),
        "IOPSPrice"      -> row.getAs[Double]("IOPSPrice"),
        "ThroughputPrice"-> row.getAs[Double]("ThroughputPrice"),
        "pricePerGib"    -> row.getAs[Double]("pricePerGib"),
        "pricePerIOPS"   -> row.getAs[Double]("pricePerIOPS"),
        "pricePerMbps"   -> row.getAs[Double]("pricePerMbps")
      )

      val priceMapExpr = map(
        lit("StoragePrice")    , lit(priceMap("StoragePrice")),
        lit("IOPSPrice")       , lit(priceMap("IOPSPrice")),
        lit("ThroughputPrice") , lit(priceMap("ThroughputPrice")),
        lit("pricePerGib")     , lit(priceMap("pricePerGib")),
        lit("pricePerIOPS")    , lit(priceMap("pricePerIOPS")),
        lit("pricePerMbps")    , lit(priceMap("pricePerMbps"))
      )

      val diskPricingFiltered = storagePricingDF
        .filter(col("type") === "Consumption" && col("unitOfMeasure") === "1/Month" && !col("meterName").contains("Free"))
        .withColumn("mappedProductName",
            when(col("productName").contains("Standard"), lit("Standard"))
            .when(col("productName").contains("Premium"), lit("Premium"))
            .when(col("productName").contains("Ultra"), lit("Ultra"))
            .otherwise(lit("Other")) 
        ).filter(col("mappedProductName") !== "Other")

      val groupedPricing = diskPricingFiltered
        .groupBy("meterName", "mappedProductName")
        .agg(
          min("retailPrice").as("minRetailPrice")
        )

      val diskMapEntries = groupedPricing
        .select(
          concat_ws("|", col("meterName"), col("mappedProductName")).as("key"),
          col("minRetailPrice").as("value")
        )
        .collect()
        .flatMap(row => Seq(lit(row.getString(0)), lit(row.getDouble(1))))

      val diskPriceExpr = map(diskMapEntries: _*)

      val enrichedWithDiskTransform = platformDf.withColumn(
        "SkuRecommendationForServers",
         transform(col("SkuRecommendationForServers"), server =>
         server.withField(
           "SkuRecommendationResults",
           transform(server.getField("SkuRecommendationResults"), sku =>
             sku.withField(
               "disks",
                transform(sku.getField("disks"), result =>
                   result.withField("DiskMeter",
                       when(result.getField("Type") === "PremiumSSD", concat(result.getField("Size"), lit(" LRS Disk")))
                       .otherwise(concat(result.getField("Size"), lit(" Disks")))
                   )
                   .withField("DiskProductName",
                       when(result.getField("Type").isin("StandardHDD", "StandardSSD"), "Standard")
                       when(result.getField("Type").isin("PremiumSSD", "PremiumSSDV2"), "Premium")
                       when(result.getField("Type").isin("UltraSSD"), "Ultra")
                   )
                )
              )
            )
          )
        )
      )

      val enrichedWithSSDV2 = enrichedWithDiskTransform.withColumn(
        "SkuRecommendationForServers",
         transform(col("SkuRecommendationForServers"), server =>
         server.withField(
           "SkuRecommendationResults",
           transform(server.getField("SkuRecommendationResults"), sku =>
             sku.withField(
               "disks",
                transform(sku.getField("disks"), result =>
                   result.withField("PremiumSSDV2StorageCost",
                       when(result.getField("Type") === "PremiumSSDV2", element_at(priceMapExpr, lit("pricePerGib")) * result.getField("MaxSizeInGib")).otherwise(0.0)
                   )
                   .withField("PremiumSSDV2IOPSCost",
                       when(result.getField("Type") === "PremiumSSDV2", element_at(priceMapExpr, lit("pricePerIOPS")) * result.getField("MaxIOPS") - 3000).otherwise(0.0)
                   )
                   .withField("PremiumSSDV2ThroughputCost",
                       when(result.getField("Type") === "PremiumSSDV2", element_at(priceMapExpr, lit("pricePerMbps")) * result.getField("MaxThroughputInMbps") - 125).otherwise(0.0)
                   )
                )
              )
            )
           )
          )
        )

      val enrichedWithSSDV2TotalCost = enrichedWithSSDV2.withColumn(
        "SkuRecommendationForServers",
        transform(col("SkuRecommendationForServers"), server =>
          server.withField(
            "SkuRecommendationResults",
            transform(server.getField("SkuRecommendationResults"), sku =>
              sku.withField(
                "disks",
                transform(sku.getField("disks"), result =>
                  result.withField(
                    "TotalPremiumSSDV2Cost",
                    when(
                      result.getField("Type") === "PremiumSSDV2",
                      result.getField("PremiumSSDV2StorageCost") +
                        result.getField("PremiumSSDV2IOPSCost") +
                        result.getField("PremiumSSDV2ThroughputCost")
                    ).otherwise(0.0)
                  )
                )
              )
            )
          )
        )
      )

    val enrichedWithSSDCost = enrichedWithSSDV2TotalCost.withColumn(
      "SkuRecommendationForServers",
      transform(col("SkuRecommendationForServers"), server =>
        server.withField(
          "SkuRecommendationResults",
          transform(server.getField("SkuRecommendationResults"), sku =>
            sku.withField(
              "disks",
              transform(sku.getField("disks"), result =>
                result
                  .withField(
                    "TotalPremiumSSDCost",
                    when(
                      result.getField("Type") === "PremiumSSD",
                      element_at(
                        diskPriceExpr,
                        concat_ws("|", result.getField("DiskMeter"), result.getField("DiskProductName"))
                      )
                    ).otherwise(0.0)
                  )
                  .withField(
                    "TotalOtherDiskCost",
                    when(
                      result.getField("Type") =!= "PremiumSSD" &&
                      result.getField("Type") =!= "PremiumSSDV2",
                      element_at(
                        diskPriceExpr,
                        concat_ws("|", result.getField("DiskMeter"), result.getField("DiskProductName"))
                      )
                    ).otherwise(0.0)
                  )
              )
            )
          )
        )
      )
    )

    enrichedWithSSDCost.withColumn(
      "SkuRecommendationForServers",
      transform(col("SkuRecommendationForServers"), server =>
        server.withField(
          "SkuRecommendationResults",
          transform(server.getField("SkuRecommendationResults"), result =>
            result.withField(
              "StorageCost",
              aggregate(
                result.getField("disks"),
                lit(0).cast("double"),
                (acc, disk) =>
                  acc +
                  coalesce(disk.getField("TotalPremiumSSDV2Cost"), lit(0.0)) +
                  coalesce(disk.getField("TotalPremiumSSDCost"), lit(0.0)) +
                  coalesce(disk.getField("TotalOtherDiskCost"), lit(0.0)),
                acc => acc
              )
            )
          )
        )
      )
    )
  }

  // def addMonthlyCostOptions(): DataFrame => DataFrame = { df =>
  //   df.withColumn("monthlyCostOptions", array(
  //     buildMonthlyCostStruct("With1YearASPAndDevTest", "computeCostASPDevTest_1Year"),
  //     buildMonthlyCostStruct("With3YearASPAndDevTest", "computeCostASPDevTest_3Years"),
  //     buildMonthlyCostStruct("With1YearASPAndProd", "computeCostASPProd_1Year"),
  //     buildMonthlyCostStruct("With3YearASPAndProd", "computeCostASPProd_3Years"),
  //     buildMonthlyCostStruct("With1YearRIAndProd", "computeCostRI_1Year"),
  //     buildMonthlyCostStruct("With3YearRIAndProd", "computeCostRI_3Years"),
  //   ))
  // }

  def addMonthlyCostOptions(): DataFrame => DataFrame = { df =>

    df.withColumn(
      "SkuRecommendationForServers",
      transform(col("SkuRecommendationForServers"), server =>
        server.withField(
          "SkuRecommendationResults",
          transform(server.getField("SkuRecommendationResults"), result =>
            result.withField(
              "monthlyCostOptions",
              array(
                buildMonthlyCostStruct("With1YearASPAndDevTest", result.getField("computeCostASPDevTest_1Year"), result.getField("storageCost")),
                buildMonthlyCostStruct("With3YearASPAndDevTest", result.getField("computeCostASPDevTest_3Years"), result.getField("storageCost")),
                buildMonthlyCostStruct("With1YearASPAndProd", result.getField("computeCostASPProd_1Year"), result.getField("storageCost")),
                buildMonthlyCostStruct("With3YearASPAndProd", result.getField("computeCostASPProd_3Years"), result.getField("storageCost")),
                buildMonthlyCostStruct("With1YearRIAndProd", result.getField("computeCostRI_1Year"), result.getField("storageCost")),
                buildMonthlyCostStruct("With3YearRIAndProd", result.getField("computeCostRI_3Years"), result.getField("storageCost")),
              )
            )
          )
        )
      ))
  }

  // Helpers
  private def reservationTermToColName(term: String): String = term.replaceAll("[^A-Za-z0-9]", "")
  private def reservationTermToFactor(term: String): Double = term match {
    case "1 Year" => 12.0
    case "3 Years" => 36.0
    case _      => 12.0
  }

  private def calculateMonthlyCost(column: Column, factor: Double, op: (Column, Double) => Column): Column =
    round(op(column, factor), 2)

  private def computeMonthlyCost(priceColumn: Column, months: Double): Column =
    round(priceColumn / months, 2)

  private def buildMonthlyCostStruct(label: String, computeCost: Column, storageCost: Column): Column =
    struct(
      lit(label).as("keyName"),
      struct(
        computeCost.as("computeCost"),
        storageCost.as("storageCost"),
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
