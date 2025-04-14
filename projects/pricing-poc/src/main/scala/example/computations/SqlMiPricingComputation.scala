package example.computations

import org.apache.spark.sql.{DataFrame, SparkSession}
import example.constants.PlatformType
import example.loader.PricingDataLoader
import example.pricing.PaaSPricing

class SqlMiPricingComputation extends PricingComputation {
  override def compute(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val pricing = PricingDataLoader.load(PlatformType.AzureSqlManagedInstance)
    val computeDF = pricing.getOrElse("Compute", throw new RuntimeException("Compute pricing missing"))
    val storageDF = pricing.getOrElse("Storage", throw new RuntimeException("Storage pricing missing"))

    df.transform(PaaSPricing.transformPlatform())
      .transform(PaaSPricing.enrichWithStoragePricing(storageDF))
      .transform(PaaSPricing.enrichWithReservedPricing(computeDF, "1 Year"))
      .transform(PaaSPricing.enrichWithReservedPricing(computeDF, "3 Years"))
      .transform(PaaSPricing.addMonthlyCostOptions())
  }
}
