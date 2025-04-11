package example.computations

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import example.reader.JsonReader
import example.constants.PlatformType
import java.nio.file.Paths
import example.strategy.ReservedProdPaaSPricing
import example.pricing.{PaaSPricing, IaaSPricing}
import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames

object PricingComputationsV1 {


  def computePricingForSqlDB()(df: DataFrame): DataFrame = {
    implicit val spark: SparkSession = df.sparkSession
    val pricingDataFrames = loadPricingDataFrames(PlatformType.AzureSqlDatabase)
    val computeDataFrame = pricingDataFrames.get("Compute").getOrElse(throw new RuntimeException(s"Compute pricing data not found"))
    val storageDataFrame = loadPricingDataFrames(PlatformType.AzureSqlManagedInstance).get("Storage").getOrElse(throw new RuntimeException(s"Storage pricing data not found"))

    df.transform(PaaSPricing.transformPlatform())
      .transform(PaaSPricing.enrichWithReservedPricing(computeDataFrame, storageDataFrame, "1 Year"))
      .transform(PaaSPricing.enrichWithReservedPricing(computeDataFrame, storageDataFrame, "3 Years"))
      .transform(PaaSPricing.addMonthlyCostOptions())
  }

  def computePricingForSqlMI()(df: DataFrame): DataFrame = {
    implicit val spark: SparkSession = df.sparkSession
    val pricingDataFrames = loadPricingDataFrames(PlatformType.AzureSqlManagedInstance)
    val computeDataFrame = pricingDataFrames.get("Compute").getOrElse(throw new RuntimeException(s"Compute pricing data not found"))
    val storageDataFrame = pricingDataFrames.get("Storage").getOrElse(throw new RuntimeException(s"Storage pricing data not found"))
    
    df.transform(PaaSPricing.transformPlatform())
      .transform(PaaSPricing.enrichWithReservedPricing(computeDataFrame, storageDataFrame, "1 Year"))
      .transform(PaaSPricing.enrichWithReservedPricing(computeDataFrame, storageDataFrame, "3 Years"))
      .transform(PaaSPricing.addMonthlyCostOptions())
  }

  def computePricingForSqlVM()(df: DataFrame): DataFrame = {
    implicit val spark: SparkSession = df.sparkSession
    val pricingDataFrames = loadPricingDataFrames(PlatformType.AzureSqlVirtualMachine)
    val computeDataFrame = pricingDataFrames.get("Compute").getOrElse(throw new RuntimeException(s"Compute pricing data not found"))
    val storageDataFrame = pricingDataFrames.get("Storage").getOrElse(throw new RuntimeException(s"Storage pricing data not found"))
    
    df.transform(IaaSPricing.transformPlatform())
      .transform(IaaSPricing.enrichWithStoragePricing(storageDataFrame))
      .transform(IaaSPricing.enrichWithReservedPricing(computeDataFrame, "1 Year"))
      .transform(IaaSPricing.enrichWithReservedPricing(computeDataFrame, "3 Years"))
      .transform(IaaSPricing.enrichWithAspProdPricing(computeDataFrame, "1 Year"))
      .transform(IaaSPricing.enrichWithAspProdPricing(computeDataFrame, "3 Years"))
      .transform(IaaSPricing.enrichWithAspDevTestPricing(computeDataFrame, "1 Year"))
      .transform(IaaSPricing.enrichWithAspDevTestPricing(computeDataFrame, "3 Years"))
      .transform(IaaSPricing.addMonthlyCostOptions())
  }

  def loadPricingDataFrames(platformType: PlatformType)(implicit spark: SparkSession): Map[String, DataFrame] = {
    val fileMappings: Map[PlatformType, Map[String, String]] = Map(
      PlatformType.AzureSqlDatabase -> Map(
        "Compute" -> "SQL_DB_Compute.json",
        // "License" -> "SQL_DB_License.json",
        "Storage" -> "SQL_DB_Storage.json"
      ),
      PlatformType.AzureSqlManagedInstance -> Map(
        "Compute" -> "SQL_MI_Compute.json",
        // "License" -> "SQL_MI_License.json",
        "Storage" -> "SQL_MI_Storage.json"
      ),
      PlatformType.AzureSqlVirtualMachine -> Map(
        "Compute" -> "SQL_VM_Compute.json",
        "Storage" -> "SQL_VM_Storage.json"
      )
    )

    val pricingDataFolderPath = Paths.get(System.getProperty("user.dir"), "src", "main", "resources", "pricing").toString
    val fileMapping = fileMappings.getOrElse(platformType, Map.empty)

    fileMapping.map { case (category, fileName) =>
      val filePath = s"$pricingDataFolderPath/$fileName"
      val rawDF = JsonReader.readJson(spark, filePath)
      val contentDF = rawDF.selectExpr("explode(Content) as Content").select("Content.*")

      category -> contentDF
    }
  }
}
