package example.calculator

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import example.constants.{AzureSqlPaaSServiceTier, RecommendationConstants, AzureSqlPaaSHardwareType, PricingType}

class PaasPricingCalculator extends PricingCalculator {

  override def calculateReservedComputeCost(platformDf: DataFrame, pricingDf: DataFrame, reservationTerm: String): Double = {
    val flattenedDf = platformDf
      .withColumn("SkuRecommendationForServers", explode(col("SkuRecommendationForServers")))
      .withColumn("SkuRecommendationResults", explode(col("SkuRecommendationForServers.SkuRecommendationResults")))
      .select(
        col("SkuRecommendationForServers.ServerName"),
        col("SkuRecommendationResults.TargetSku"))
    
    flattenedDf.printSchema()
    flattenedDf.show(false)

    val sqlServiceTierOpt: Option[String] = flattenedDf
      .select(col("TargetSku.Category.SqlServiceTier"))
      .collect()
      .headOption
      .map(_.getString(0))

    val normalizedSqlServiceTierOpt = sqlServiceTierOpt.map(_.trim)
    
    val sqlServiceTier: Option[AzureSqlPaaSServiceTier.Value] =
      AzureSqlPaaSServiceTier.values.find(_.toString.trim == normalizedSqlServiceTierOpt.getOrElse(""))

    val sqlHardwareTypeOpt: Option[String] = flattenedDf
      .select(col("TargetSku.Category.HardwareType"))
      .collect()
      .headOption
      .map(_.getString(0))

    val normalizedSqlHardwareTypeOpt = sqlHardwareTypeOpt.map(_.trim)

    val sqlHardwareType: Option[AzureSqlPaaSHardwareType.Value] =
      AzureSqlPaaSHardwareType.values.find(_.toString == normalizedSqlHardwareTypeOpt.getOrElse(""))    

    val filteredDf = pricingDf
      .filter(
        col("skuName") === "vCore" &&
        col("productName").contains(getSqlPaaSTier(sqlServiceTier)) &&
        col("productName").contains(getSqlPaasHardwareGeneration(sqlHardwareType)) &&
        col("location") === "US West" &&
        col("type") === PricingType.Reservation.toString &&
        col("reservationTerm") === reservationTerm &&
        col("UnitOfMeasure") === "1 Hour"
    )

    filteredDf.printSchema()
    filteredDf.show(false)

    var minPrice = -1.0
    val retailPrice = if (!filteredDf.isEmpty) {
        minPrice = filteredDf.orderBy("retailPrice").select("retailPrice").first().getDouble(0)
       // minPrice * azureSqlSku.ComputeSize * 24 * 30.5
    } 
    minPrice
  }

  override def calculateReservedStorageCost(platformDf: DataFrame, pricingDf: DataFrame): Double = 0.18

  override def calculateDevTestReservedComputeCost(platformDf: DataFrame, pricingDf: DataFrame, reservationTerm: String): Double = ???

  override def calculateDevTestReservedStorageCost(platformDf: DataFrame, pricingDf: DataFrame): Double = ???

  private def getSqlPaaSTier(tier: Option[AzureSqlPaaSServiceTier.Value]): String = tier match {
    case Some(AzureSqlPaaSServiceTier.GeneralPurpose) | Some(AzureSqlPaaSServiceTier.NextGenGeneralPurpose) =>
      RecommendationConstants.GeneralPurpose
  
    case Some(AzureSqlPaaSServiceTier.BusinessCritical) =>
      RecommendationConstants.BusinessCritical
  
    case Some(AzureSqlPaaSServiceTier.HyperScale) =>
      RecommendationConstants.Hyperscale
  
    case _ => ""
  }


  private def getSqlPaasHardwareGeneration(tier: Option[AzureSqlPaaSHardwareType.Value]): String = tier match {
    case Some(AzureSqlPaaSHardwareType.Gen5) =>
        RecommendationConstants.Gen5
    
    case Some(AzureSqlPaaSHardwareType.PremiumSeries) =>
        RecommendationConstants.PremiumSeries
    
    case Some(AzureSqlPaaSHardwareType.PremiumSeriesMemoryOptimized) =>
        RecommendationConstants.PremiumSeriesMemoryOptimized
    
    case _ => ""
  }
  
}
