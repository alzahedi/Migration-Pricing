package example.pricing

import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions._
import example.constants.{AzureSqlPaaSServiceTier, RecommendationConstants, AzureSqlPaaSHardwareType, PricingType, PlatformType}

object PaaSPricing {
  // Entry point to transform the incoming platform DataFrame
  // def transformPlatform(): DataFrame => DataFrame = { df =>
  //   df
  //     .withColumn("SkuRecommendationForServers", explode(col("SkuRecommendationForServers")))
  //     .withColumn("SkuRecommendationResults", explode(col("SkuRecommendationForServers.SkuRecommendationResults")))
  //     .transform(addServiceTier)
  //     .transform(addHardwareType)
  //     .withColumn("computeSize", col("SkuRecommendationResults.TargetSku.ComputeSize"))
  //     .withColumn("storageMaxSizeInMb", col("SkuRecommendationResults.TargetSku.StorageMaxSizeInMb"))
  //     .withColumn("targetPlatform", col("SkuRecommendationResults.TargetSku.Category.SqlTargetPlatform"))
  //     .withColumn("databaseName", col("SkuRecommendationResults.DatabaseName"))
  //     .withColumn("storageMaxSizeInGb", col("SkuRecommendationResults.TargetSku.StorageMaxSizeInMb") / 1024)
  // }

  def transformPlatform(): DataFrame => DataFrame = { df => 
    df.withColumn(
      "SkuRecommendationForServers",
      transform(col("SkuRecommendationForServers"), server =>
        server.withField(
          "SkuRecommendationResults",
          transform(server.getField("SkuRecommendationResults"), result =>
            result
              .withField(
                "sqlServiceTier",
                when(result.getField("TargetSku").getField("Category").getField("SqlServiceTier")
                  .isin("General Purpose", "Next-Gen General Purpose"), "General Purpose")
                  .when(result.getField("TargetSku").getField("Category").getField("SqlServiceTier") === "Business Critical", "Business Critical")
                  .when(result.getField("TargetSku").getField("Category").getField("SqlServiceTier") === "Hyperscale", "Hyperscale")
                  .otherwise("Unknown")
              )
              .withField(
                "sqlHardwareType",
                when(result.getField("TargetSku").getField("Category").getField("HardwareType") === "Gen5", "Gen5")
                  .when(result.getField("TargetSku").getField("Category").getField("HardwareType") === "Premium Series", "Premium Series Compute")
                  .when(result.getField("TargetSku").getField("Category").getField("HardwareType") === "Premium Series - Memory Optimized", "Premium Series Memory Optimized Compute")
                  .otherwise("Unknown")
              )
              .withField(
                "targetPlatform",
                result.getField("TargetSku").getField("Category").getField("SqlTargetPlatform")
              )
              .withField(
                "storageMaxSizeInGb",
                result.getField("TargetSku").getField("StorageMaxSizeInMb") / 1024
              )
              .withField(
                "computeSize",
                result.getField("TargetSku").getField("ComputeSize")
              )
          )
        )
      )
    )
  }

  private def addServiceTier(df: DataFrame): DataFrame = {
    df.withColumn("sqlServiceTier",
      when(col("SkuRecommendationResults.TargetSku.Category.SqlServiceTier").isin("General Purpose", "Next-Gen General Purpose"), "General Purpose")
        .when(col("SkuRecommendationResults.TargetSku.Category.SqlServiceTier") === "Business Critical", "Business Critical")
        .when(col("SkuRecommendationResults.TargetSku.Category.SqlServiceTier") === "Hyperscale", "Hyperscale")
        .otherwise("Unknown")
    )
  }

  private def addHardwareType(df: DataFrame): DataFrame = {
    df.withColumn("sqlHardwareType",
      when(col("SkuRecommendationResults.TargetSku.Category.HardwareType") === "Gen5", "Gen5")
        .when(col("SkuRecommendationResults.TargetSku.Category.HardwareType") === "Premium Series", "Premium Series Compute")
        .when(col("SkuRecommendationResults.TargetSku.Category.HardwareType") === "Premium Series - Memory Optimized", "Premium Series Memory Optimized Compute")
        .otherwise("Unknown")
    )
  }

  // def enrichWithStoragePricing(storagePricingDF: DataFrame): DataFrame => DataFrame = { platformDf => 
  //     val filteredStorageDf = storagePricingDF
  //       .filter(
  //         col("location") === "US West" &&
  //         col("type") === PricingType.Consumption.toString
  //       )
  //       .select(col("skuName"), col("retailPrice"))
      
  //     platformDf
  //       .join(filteredStorageDf, col("sqlServiceTier") === col("skuName"))
  //       .withColumn("storageCost", calculateStorageCost())
  // }

    def enrichWithStoragePricing(storagePricingDF: DataFrame): DataFrame => DataFrame = { platformDf => 

      val filteredStorageDf = storagePricingDF
        .filter(
          col("location") === "US West" &&
          col("type") === PricingType.Consumption.toString
        )
        .select(col("skuName"), col("retailPrice"))

      val storageMapExprEntries = filteredStorageDf
        .select(
          col("skuName").as("key"),
          col("retailPrice").as("value")
        )
        .collect()
        .flatMap(row => Seq(lit(row.getString(0)), lit(row.getDouble(1))))

      val storagePricingExpr = map(storageMapExprEntries: _*)
      
      platformDf.withColumn(
        "SkuRecommendationForServers",
        transform(col("SkuRecommendationForServers"), server =>
          server.withField(
            "SkuRecommendationResults",
            transform(server.getField("SkuRecommendationResults"), result =>
              result.withField(
                  "storageCost",
                  bround(
                    when(
                      result.getField("targetPlatform") === PlatformType.AzureSqlManagedInstance.toString,
                      coalesce(element_at(storagePricingExpr, result.getField("sqlServiceTier")), lit(0.0)) *
                        greatest(result.getField("storageMaxSizeInGb") - 32, lit(0))
                    ).otherwise(
                      coalesce(element_at(storagePricingExpr, result.getField("sqlServiceTier")), lit(0.0)) *
                        (result.getField("storageMaxSizeInGb") * 1.3)
                    ),
                    2
                  )
              )
            )
          )
        )
      )
  }

  def enrichWithLicensePricing(licenseDF: DataFrame): DataFrame => DataFrame = { platformDf => 

    licenseDF.show(false)
    val filteredLicenseDF = licenseDF
      .filter(
        col("type") === PricingType.Consumption.toString
      )
      .withColumn("sqlServiceTier",
        when(col("productName").contains("General Purpose"), "General Purpose")
          .when(col("productName").contains("Business Critical"), "Business Critical")
          .when(col("productName").contains("Hyperscale"), "Hyperscale")
          .otherwise("Unknown")
      )
      .groupBy("sqlServiceTier")
      .agg(min("retailPrice").alias("minRetailPrice"))

      filteredLicenseDF.show(false)

      val licenseMapEntries = filteredLicenseDF
        .select(
          col("sqlServiceTier").as("key"),
          col("minRetailPrice").as("value")
        )
        .collect()
        .flatMap(row => Seq(lit(row.getString(0)), lit(row.getDouble(1))))

      val licensePricingExpr = map(licenseMapEntries: _*)

      val resultDF = platformDf.withColumn(
        "SkuRecommendationForServers",
        transform(col("SkuRecommendationForServers"), server =>
          server.withField(
            "SkuRecommendationResults",
            transform(server.getField("SkuRecommendationResults"), result =>
              result.withField(
                  "MonthlyCost",
                    result.getField("MonthlyCost").withField(
                      "SqlLicenseCost",
                      calculateMonthlyCost(result.getField("computeSize") *
                      coalesce(
                        element_at(
                        licensePricingExpr,
                         result.getField("sqlServiceTier")),
                         lit(0.0)
                      ), 
                      24 * 30.5,
                      _*_
                      ),
                )
              )
            )
          )
        )
      )
      //resultDF.printSchema()
      resultDF
  }

  // def enrichWithReservedPricing(pricingDf: DataFrame, reservationTerm: String): DataFrame => DataFrame = { platformDf =>
    
  //   val computeCostDf = platformDf
  //     .join(getMinComputePrice(pricingDf, reservationTerm), Seq("sqlServiceTier", "sqlHardwareType"), "inner")

  //   computeCostDf      
  //     .withColumn(s"computeCost_${reservationTermToColName(reservationTerm)}",
  //       computeMonthlyCost(
  //         col("computeSize") * col(s"minRetailPrice_$reservationTerm"),
  //         reservationTermToFactor(reservationTerm)
  //       )
  //     )
  // }

  
  def enrichWithReservedPricing(pricingDf: DataFrame, reservationTerm: String): DataFrame => DataFrame = { platformDf =>
    
    val pricingMapExprEntries = getMinComputePrice(pricingDf, reservationTerm)
      .select(
        concat_ws("|", col("sqlServiceTier"), col("sqlHardwareType")).as("key"),
        col(s"minRetailPrice_$reservationTerm").as("value")
      )
      .collect()
      .flatMap(row => Seq(lit(row.getString(0)), lit(row.getDouble(1))))

    val pricingExpr = map(pricingMapExprEntries: _*)

    platformDf.withColumn(
      "SkuRecommendationForServers",
      transform(col("SkuRecommendationForServers"), server =>
        server.withField(
          "SkuRecommendationResults",
          transform(server.getField("SkuRecommendationResults"), result =>
            result.withField(
                s"computeCost_${reservationTermToColName(reservationTerm)}",
                calculateMonthlyCost(result.getField("computeSize") *
                coalesce(
                  element_at(
                  pricingExpr,
                  concat_ws("|", result.getField("sqlServiceTier"), result.getField("sqlHardwareType"))
                ), 
                lit(0.0)),
                reservationTermToFactor(reservationTerm),
                _/_
                )
              )
            )
          )
        )
      )
  }

  private def getMinComputePrice(pricingDf: DataFrame, reservationTerm: String): DataFrame = {
    pricingDf
      .filter(
        col("skuName") === "vCore" &&
        col("location") === "US West" &&
        col("type") === PricingType.Reservation.toString &&
        col("reservationTerm") === reservationTerm &&
        col("UnitOfMeasure") === "1 Hour"
      )
      .withColumn("sqlServiceTier",
        when(col("productName").contains("General Purpose"), "General Purpose")
          .when(col("productName").contains("Business Critical"), "Business Critical")
          .when(col("productName").contains("Hyperscale"), "Hyperscale")
          .otherwise("Unknown")
      )
      .withColumn("sqlHardwareType",
        when(col("productName").contains("Gen5"), "Gen5")
          .when(col("productName").contains("Premium Series Compute"), "Premium Series Compute")
          .when(col("productName").contains("Premium Series Memory Optimized Compute"), "Premium Series Memory Optimized Compute")
          .otherwise("Unknown")
      )
      .groupBy("sqlServiceTier", "sqlHardwareType")
      .agg(min("retailPrice").alias(s"minRetailPrice_$reservationTerm"))
  }

  private def calculateStorageCost(): Column = {
    bround(
      when(col("targetPlatform") === PlatformType.AzureSqlManagedInstance.toString,
        col("retailPrice") * greatest(col("storageMaxSizeInGb") - 32, lit(0))
      ).otherwise(
        col("retailPrice") * (col("storageMaxSizeInGb") * 1.3)
      ),
      2
    )
  }

  // private def computeMonthlyCost(priceColumn: Column, months: Double): Column = {
  //   round(priceColumn / months, 2)
  // }

  
  private def calculateMonthlyCost(column: Column, factor: Double, op: (Column, Double) => Column): Column =
    round(op(column, factor), 2)

  // def addMonthlyCostOptions(): DataFrame => DataFrame = { df =>
  //   df.withColumn("monthlyCostOptions", array(
  //     struct(
  //       lit("With1YearRIAndProd").as("keyName"),
  //       struct(
  //         col("computeCost_1Yr").as("computeCost"),
  //         col("storageCost"),
  //         lit(0.0).as("iopsCost")
  //       ).as("keyValue")
  //     ),
  //     struct(
  //       lit("With3YearRIAndProd").as("keyName"),
  //       struct(
  //         col("computeCost_3Yr").as("computeCost"),
  //         col("storageCost"),
  //         lit(0.0).as("iopsCost")
  //       ).as("keyValue")
  //     )
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
                struct(
                  lit("With1YearRIAndProd").as("keyName"),
                  struct(
                    result.getField("computeCost_1Yr").as("computeCost"),
                    result.getField("storageCost").as("storageCost"),
                    lit(0.0).as("iopsCost")
                  ).as("keyValue")
                ),
                struct(
                  lit("With3YearRIAndProd").as("keyName"),
                  struct(
                    result.getField("computeCost_3Yr").as("computeCost"),
                    result.getField("storageCost").as("storageCost"),
                    lit(0.0).as("iopsCost")
                  ).as("keyValue")
                )
              )
            )
          )
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