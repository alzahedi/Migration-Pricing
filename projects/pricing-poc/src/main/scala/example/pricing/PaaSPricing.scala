package example.pricing

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import example.constants.{AzureSqlPaaSServiceTier, RecommendationConstants, AzureSqlPaaSHardwareType, PricingType}
import example.constants.PlatformType
import org.apache.spark.sql.Column

object PaaSPricing{

    def transformPlatformDF()(df: DataFrame): DataFrame = {
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

    def getMinPricingDf(pricingDf: DataFrame, reservationTerm: String): DataFrame = {
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

    def withRIProdCost(pricingDF: DataFrame, storageDF: DataFrame, reservationTerm: String)(platformDF: DataFrame): DataFrame = {
        val minPricingDF = getMinPricingDf(pricingDF, reservationTerm)

        val computeCostDF = platformDF.join(minPricingDF, Seq("sqlServiceTier", "sqlHardwareType"), "inner").as("compute")

        val storagePricingDf = storageDF
            .filter(
              col("location") === "US West" &&
              col("type") === PricingType.Consumption.toString
            )
            .select(col("skuName"), col("retailPrice"))
            .as("storage")

        computeCostDF
            .join(
              storagePricingDf,
              col("compute.sqlServiceTier") === col("storage.skuName"),
              "inner"
            )
            .withColumn("storageCost",
              bround(
                when(col("compute.targetPlatform") === PlatformType.AzureSqlManagedInstance.toString,
                  col("storage.retailPrice") * greatest(col("compute.storageMaxSizeInGb") - 32, lit(0))
                ).otherwise(
                  col("storage.retailPrice") * (col("compute.storageMaxSizeInGb") * 1.3)
                ),
                2
              )
            )
            .withColumn(s"computeCost_${reservationTermToColName.getOrElse(reservationTerm, "")}",
              withMonthlyCost(
                col("compute.computeSize") * col(s"compute.minRetailPrice_${reservationTerm}"),
                reservationTermToFactor.getOrElse(reservationTerm, 0),
                _ / _
              )
            )
    }

    def withMonthlyCostOptions()(platformDF: DataFrame): DataFrame = {
        platformDF.withColumn("monthlyCostOptions", array(
        struct(
          lit("With1YearRIAndProd").as("keyName"),
          struct(
            col("computeCost_1Yr").as("computeCost"),
            col("storageCost"),
            lit(0.0).as("iopsCost")
          ).as("keyValue")
        ),
        struct(
          lit("With3YearRIAndProd").as("keyName"),
          struct(
            col("computeCost_3Yr").as("computeCost"),
            col("storageCost"),
            lit(0.0).as("iopsCost")
          ).as("keyValue")
        )
      ))
    }

    def withMonthlyCost(column: Column, factor: Double, operation: (Column, Double) => Column): Column = {
        round(operation(column, factor), 2)
    }

    val reservationTermToColName: Map[String, String] = Map(
        "1 Year"  -> "1Yr",
        "3 Years"  -> "3Yr",
    )

    val reservationTermToFactor: Map[String, Double] = Map(
        "1 Year"  -> 12.0,
        "3 Years"  -> 36.0,
    )
 }