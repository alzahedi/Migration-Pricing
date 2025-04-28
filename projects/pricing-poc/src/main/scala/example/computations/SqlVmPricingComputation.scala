package example.computations

import org.apache.spark.sql.{DataFrame, SparkSession}
import example.constants.PlatformType
import example.loader.PricingDataLoader
import example.pricing.IaaSPricing

class SqlVmPricingComputation(spark: SparkSession) extends PricingComputation {
  override def compute(df: DataFrame, computeDF: DataFrame, storageDF: DataFrame, licenseDF: DataFrame = null): DataFrame = {
    // val computeDF = PricingDataLoader(PlatformType.AzureSqlVirtualMachine, "Compute", spark).load()
    // val storageDF = PricingDataLoader(PlatformType.AzureSqlVirtualMachine, "Storage", spark).load()

    df.transform(IaaSPricing.transformPlatform())
      .transform(IaaSPricing.enrichWithStoragePricing(storageDF))
      .transform(IaaSPricing.enrichWithReservedPricing(computeDF, "1 Year"))
      .transform(IaaSPricing.enrichWithReservedPricing(computeDF, "3 Years"))
      .transform(IaaSPricing.enrichWithAspProdPricing(computeDF, "1 Year"))
      .transform(IaaSPricing.enrichWithAspProdPricing(computeDF, "3 Years"))
      .transform(IaaSPricing.enrichWithAspDevTestPricing(computeDF, "1 Year"))
      .transform(IaaSPricing.enrichWithAspDevTestPricing(computeDF, "3 Years"))
      .transform(IaaSPricing.addMonthlyCostOptions())
  }
}

