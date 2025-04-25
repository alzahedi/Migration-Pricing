package example.computations

import org.apache.spark.sql.{DataFrame, SparkSession}
import example.constants.PlatformType
import example.loader.PricingDataLoader
import example.pricing.PaaSPricing

class SqlMiPricingComputation(spark: SparkSession) extends PricingComputation {
  override def compute(df: DataFrame, computeDF: DataFrame, storageDF: DataFrame): DataFrame = {
    // val computeDF = PricingDataLoader(PlatformType.AzureSqlManagedInstance, "Compute", spark).load()
    // val storageDF = PricingDataLoader(PlatformType.AzureSqlManagedInstance, "Storage", spark).load()

    df.transform(PaaSPricing.transformPlatform())
      .transform(PaaSPricing.enrichWithStoragePricing(storageDF))
      .transform(PaaSPricing.enrichWithReservedPricing(computeDF, "1 Year"))
      .transform(PaaSPricing.enrichWithReservedPricing(computeDF, "3 Years"))
      .transform(PaaSPricing.addMonthlyCostOptions())
  }
}
