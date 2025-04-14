package example.computations

import org.apache.spark.sql.{DataFrame, SparkSession}
import example.constants.PlatformType
import example.loader.PricingDataLoader
import example.pricing.IaaSPricing

class SqlVmPricingComputation extends PricingComputation {
  override def compute(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val pricing = PricingDataLoader.load(PlatformType.AzureSqlVirtualMachine)
    val computeDF = pricing.getOrElse("Compute", throw new RuntimeException("Compute pricing missing"))
    val storageDF = pricing.getOrElse("Storage", throw new RuntimeException("Storage pricing missing"))

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

