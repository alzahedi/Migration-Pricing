package example.strategy

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import example.constants.{AzureSqlPaaSServiceTier, RecommendationConstants, AzureSqlPaaSHardwareType, PricingType}
import example.constants.PlatformType
import org.apache.spark.sql.Column

class ReservedProdPaaSPricing extends PricingStrategy {

  private def normalizePlatformDf(df: DataFrame): DataFrame = {
    df.withColumn("SkuRecommendationForServers", explode(col("SkuRecommendationForServers")))
      .withColumn("SkuRecommendationResults", explode(col("SkuRecommendationForServers.SkuRecommendationResults")))
      .withColumn("sqlServiceTier",
        when(col("SkuRecommendationResults.TargetSku.Category.SqlServiceTier").isin("General Purpose", "Next-Gen General Purpose"), "General Purpose")
          .when(col("SkuRecommendationResults.TargetSku.Category.SqlServiceTier") === "Business Critical", "Business Critical")
          .when(col("SkuRecommendationResults.TargetSku.Category.SqlServiceTier") === "Hyperscale", "Hyperscale")
          .otherwise("Unknown")
      )
      .withColumn("sqlHardwareType",
        when(col("SkuRecommendationResults.TargetSku.Category.HardwareType") === "Gen5", "Gen5")
          .when(col("SkuRecommendationResults.TargetSku.Category.HardwareType") === "Premium Series", "Premium Series Compute")
          .when(col("SkuRecommendationResults.TargetSku.Category.HardwareType") === "Premium Series - Memory Optimized", "Premium Series Memory Optimized Compute")
          .otherwise("Unknown")
      )
      .withColumn("computeSize", col("SkuRecommendationResults.TargetSku.ComputeSize"))
      .withColumn("storageMaxSizeInMb", col("SkuRecommendationResults.TargetSku.StorageMaxSizeInMb"))
      .withColumn("targetPlatform", col("SkuRecommendationResults.TargetSku.Category.SqlTargetPlatform"))
      .withColumn("storageMaxSizeInGb", col("SkuRecommendationResults.TargetSku.StorageMaxSizeInMb") / 1024)
  }

  private def getMinPricingDf(pricingDf: DataFrame, reservationTerm: String): DataFrame = {
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

  override def computeCost(platformDf: DataFrame, pricingDf: DataFrame, storageDf: DataFrame): DataFrame = {
    val explodedDf = normalizePlatformDf(platformDf)

    val minPricing1Yr = getMinPricingDf(pricingDf, "1 Year")
    val minPricing3Yr = getMinPricingDf(pricingDf, "3 Years")

    val computeCostDf = explodedDf
      .join(minPricing1Yr, Seq("sqlServiceTier", "sqlHardwareType"), "inner")
      .join(minPricing3Yr, Seq("sqlServiceTier", "sqlHardwareType"), "inner")

    // Inline storage pricing join
    val storagePricingDf = storageDf
      .filter(
        col("location") === "US West" &&
        col("type") === PricingType.Consumption.toString
      )
      .select(col("skuName"), col("retailPrice"))

    storagePricingDf.show(false)
    val finalDf = computeCostDf
      .join(storagePricingDf, computeCostDf("sqlServiceTier") === storagePricingDf("skuName"), "inner")
      .withColumn("storageCost",
        bround(
          when(col("targetPlatform") === PlatformType.AzureSqlManagedInstance.toString,
            col("retailPrice") * greatest(col("storageMaxSizeInGb") - 32, lit(0))
          ).otherwise(
            col("retailPrice") * (col("storageMaxSizeInGb") * 1.3)
          ),
          2
        )
      )
      .withColumn("computeCost1Yr",
        calculateMonthlyCost(
          col("computeSize") * col("minRetailPrice_1 Year"),
          12.0,
          _ / _
        )
      )
      .withColumn("computeCost3Yr",
        calculateMonthlyCost(
          col("computeSize") * col("minRetailPrice_3 Years"),
          36.0,
          _ / _
        )
      )
      .withColumn("monthlyCostOptions", array(
        struct(
          lit("With1YearRIAndProd").as("keyName"),
          struct(
            col("computeCost1Yr").as("computeCost"),
            col("storageCost"),
            lit(0.0).as("iopsCost")
          ).as("keyValue")
        ),
        struct(
          lit("With3YearRIAndProd").as("keyName"),
          struct(
            col("computeCost3Yr").as("computeCost"),
            col("storageCost"),
            lit(0.0).as("iopsCost")
          ).as("keyValue")
        )
      ))

    finalDf
  }
}
