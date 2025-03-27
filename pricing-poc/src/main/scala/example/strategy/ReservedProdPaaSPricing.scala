package example.strategy

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import example.constants.{AzureSqlPaaSServiceTier, RecommendationConstants, AzureSqlPaaSHardwareType, PricingType}
import example.constants.PlatformType
import example.constants.ReservationTermToNumMap

class ReservedProdPaaSPricing extends PricingStrategy {
  override def computeCost(platformDf: DataFrame, pricingDf: DataFrame, reservationTerm: String): DataFrame = {
    val flattenedDf = platformDf
      .withColumn("SkuRecommendationForServers", explode(col("SkuRecommendationForServers")))
      .withColumn("SkuRecommendationResults", explode(col("SkuRecommendationForServers.SkuRecommendationResults")))
      .select(
        col("SkuRecommendationForServers.ServerName"),
        col("SkuRecommendationResults.TargetSku"))

    val sqlTierExpr = expr(
      "CASE " +
        "WHEN originalSqlServiceTier = 'General Purpose' OR originalSqlServiceTier = 'Next-Gen General Purpose' THEN 'General Purpose' " +
        "WHEN originalSqlServiceTier = 'Business Critical' THEN 'Business Critical' " +
        "WHEN originalSqlServiceTier = 'Hyperscale' THEN 'Hyperscale' " +
        "ELSE '' END"
    ).alias("sqlServiceTier")

    val sqlHardwareExpr = expr(
      "CASE " +
        "WHEN originalSqlHardwareType = 'Gen5' THEN 'Gen5' " +
        "WHEN originalSqlHardwareType = 'Premium Series' THEN 'Premium Series Compute' " +
        "WHEN originalSqlHardwareType = 'Premium Series - Memory Optimized' THEN 'Premium Series Memory Optimized Compute' " +
        "ELSE '' END"
    ).alias("sqlHardwareType")

    val targetSkuExpandedDF = flattenedDf
      .select(col("ServerName"), col("TargetSku.ComputeSize").alias("computeSize"), col("TargetSku.Category.SqlServiceTier").alias("originalSqlServiceTier"), col("TargetSku.Category.HardwareType").alias("originalSqlHardwareType"))
      .withColumn("sqlServiceTier", sqlTierExpr)
      .withColumn("sqlHardwareType", sqlHardwareExpr)


    val filteredPricingDF = pricingDf.filter(
       col("skuName") === "vCore" &&
       col("location") === "US West" &&
       col("type") === PricingType.Reservation.toString &&
       col("reservationTerm") === reservationTerm &&
       col("UnitOfMeasure") === "1 Hour"
    )
    val targetAlias = "target"
    val pricingAlias = "pricing"

    val joinedDF = targetSkuExpandedDF.as(targetAlias)
      .join(
        filteredPricingDF.as(pricingAlias),
        col(s"$pricingAlias.productName").contains(col(s"$targetAlias.sqlServiceTier")) &&
        col(s"$pricingAlias.productName").contains(col(s"$targetAlias.sqlHardwareType")),
        "inner"
    )
    
    val minRetailPriceDF = joinedDF
      .orderBy(col(s"$pricingAlias.retailPrice").asc)
      .limit(1)
      .select((col("retailPrice") * col("computeSize")).alias("computeCost"))
      .toDF()
  
    
    //val computeCostDF = minRetailPrice.withColumn("computeCost", col("retailPrice")).drop("retailPrice")
    //computeCostDF.show()
    //computeCostDF
    calculateMonthlyCost(minRetailPriceDF, "computeCost", 12 * ReservationTermToNumMap.map.getOrElse(reservationTerm, 0).toString.toDouble, _ / _)
  }

  override def storageCost(platformDf: DataFrame, pricingDf: DataFrame): DataFrame = {
     val flattenedDf = platformDf
      .withColumn("SkuRecommendationForServers", explode(col("SkuRecommendationForServers")))
      .withColumn("SkuRecommendationResults", explode(col("SkuRecommendationForServers.SkuRecommendationResults")))
      .select(
        col("SkuRecommendationForServers.ServerName"),
        col("SkuRecommendationResults.TargetSku"))

    val sqlTierExpr = expr(
      "CASE " +
        "WHEN originalSqlServiceTier = 'General Purpose' OR originalSqlServiceTier = 'Next-Gen General Purpose' THEN 'General Purpose' " +
        "WHEN originalSqlServiceTier = 'Business Critical' THEN 'Business Critical' " +
        "WHEN originalSqlServiceTier = 'Hyperscale' THEN 'Hyperscale' " +
        "ELSE '' END"
    ).alias("sqlServiceTier")

    val targetSkuExpandedDF = flattenedDf
      .select(col("ServerName"), col("TargetSku.Category.SqlServiceTier").alias("originalSqlServiceTier"), 
        col("TargetSku.Category.HardwareType").alias("originalSqlHardwareType"),
        col("TargetSku.StorageMaxSizeInMb").alias("storageMaxSizeInMb"),
        col("TargetSku.Category.SqlTargetPlatform").alias("targetPlatform")
      )
      .withColumn("sqlServiceTier", sqlTierExpr)
      .withColumn("storageMaxSizeInGb", col("storageMaxSizeInMb") / 1024)
    
    targetSkuExpandedDF.show(false)

    val filteredPricingDF = pricingDf.filter(
       col("location") === "US West" &&
       col("type") === PricingType.Consumption.toString 
    )
    val targetAlias = "target"
    val pricingAlias = "pricing"

    val joinedDF = targetSkuExpandedDF.as(targetAlias)
      .join(
        filteredPricingDF.as(pricingAlias),
        col(s"$pricingAlias.skuName") === col(s"$targetAlias.sqlServiceTier"),
        "inner"
    )
    
    val resultDF = joinedDF
      .orderBy(col(s"$pricingAlias.retailPrice").asc)
      .limit(1)
      .select(col("retailPrice"), col("storageMaxSizeInGb"), col("targetPlatform"))
      .withColumn(
        "storageCost",
         bround(
          when(col("targetPlatform") === PlatformType.AzureSqlManagedInstance.toString,
            col("retailPrice") * greatest(col("storageMaxSizeInGb") - 32, lit(0)) // Handles max(storageMaxSizeInGb - 32, 0)
          ).otherwise(
            col("retailPrice") * (col("storageMaxSizeInGb") * 1.3)
          ), 
        2)
      )
      .drop("retailPrice")
      .drop("storageMaxSizeInGb")
      .drop("targetPlatform")
      .toDF()
    
    resultDF.show(false)
    resultDF
  }
}

