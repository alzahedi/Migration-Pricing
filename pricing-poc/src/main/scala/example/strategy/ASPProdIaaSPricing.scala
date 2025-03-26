package example.strategy

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import example.constants.PricingType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import scala.jdk.CollectionConverters._

class ASPProdIaaSPricing extends PricingStrategy {
  override def computeCost(platformDf: DataFrame, pricingDf: DataFrame, reservationTerm: String): DataFrame = {
    val flattenedDf = platformDf
      .withColumn("SkuRecommendationForServers", explode(col("SkuRecommendationForServers")))
      .withColumn("SkuRecommendationResults", explode(col("SkuRecommendationForServers.SkuRecommendationResults")))
      .select(
        col("SkuRecommendationForServers.ServerName"),
        col("SkuRecommendationResults.TargetSku"))

    val targetSkuExpandedDF = flattenedDf
      .select(col("ServerName"), col("TargetSku.VirtualMachineSize.AzureSkuName").alias("azureSkuName"))

    val filteredPricingDF = pricingDf.filter(
        !col("SkuName").contains("Spot") && 
        !col("SkuName").contains("Low Priority") &&
        col("unitOfMeasure") === "1 Hour" &&
        col("location") === "US West" &&
        col("type") === PricingType.Consumption.toString &&
        col("savingsPlan").isNotNull
    )
    val targetAlias = "target"
    val pricingAlias = "pricing"

    val joinedDF = targetSkuExpandedDF.as(targetAlias)
      .join(
        filteredPricingDF.as(pricingAlias),
        col(s"$pricingAlias.armSkuName") === col(s"$targetAlias.azureSkuName"),
        "inner"
    )

   val explodedDF = joinedDF
      .withColumn("savingsPlan", explode(col(s"$pricingAlias.savingsPlan")))

    // Filter by reservationTerm and get the minimum retailPrice
    val minSavingsPlanDF = explodedDF
      .filter(col("savingsPlan.term") === lit(reservationTerm))
      .orderBy(col("savingsPlan.retailPrice").asc)
      .limit(1)
      .select(col("savingsPlan.retailPrice").alias("minRetailPrice"))
    
    val computeCostDF = minSavingsPlanDF.withColumn("computeCost", col("minRetailPrice")).drop("minRetailPrice")
    computeCostDF.show()

    computeCostDF
  }
  override def storageCost(platformDf: DataFrame, pricingDf: DataFrame): DataFrame = {
    // TODO: Will be replaced with actual logic
    implicit val spark: SparkSession = platformDf.sparkSession
    // Define schema
    val schema = StructType(Seq(StructField("storageCost", DoubleType, false)))
    
    // Create data as Java List
    val data = Seq(Row(0.18)).asJava
    
    // Create DataFrame
    spark.createDataFrame(data, schema)
  }
}
