package example.computations

import org.apache.spark.sql.{DataFrame, SparkSession}
import example.constants.PlatformType
import example.loader.PricingDataLoader
import example.pricing.IaaSPricing

class SqlVmPricingComputation(spark: SparkSession) extends PricingComputation {
  override def compute(df: DataFrame, computeDF: DataFrame, storageDF: DataFrame): DataFrame = {
    // val computeDF = PricingDataLoader(PlatformType.AzureSqlVirtualMachine, "Compute", spark).load()
    // val storageDF = PricingDataLoader(PlatformType.AzureSqlVirtualMachine, "Storage", spark).load()

    df.transform(example.pricing.IaaSPricing.transformPlatform())
      .transform(example.pricing.IaaSPricing.enrichWithStoragePricing(storageDF))
      .transform(example.pricing.IaaSPricing.enrichWithReservedPricing(computeDF, "1 Year"))
      .transform(example.pricing.IaaSPricing.enrichWithReservedPricing(computeDF, "3 Years"))
      .transform(example.pricing.IaaSPricing.enrichWithAspProdPricing(computeDF, "1 Year"))
      .transform(example.pricing.IaaSPricing.enrichWithAspProdPricing(computeDF, "3 Years"))
      .transform(example.pricing.IaaSPricing.enrichWithAspDevTestPricing(computeDF, "1 Year"))
      .transform(example.pricing.IaaSPricing.enrichWithAspDevTestPricing(computeDF, "3 Years"))
      .transform(example.pricing.IaaSPricing.addMonthlyCostOptions())
  }
}

