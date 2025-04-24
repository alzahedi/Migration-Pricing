package example.computations


import org.apache.spark.sql.{DataFrame, SparkSession}
import example.constants.PlatformType
import example.loader.PricingDataLoader
import example.pricing.SQLDBPricing
import org.apache.spark.sql.functions._


class SqlDbPricingComputation(spark: SparkSession) extends PricingComputation {
  override def compute(df: DataFrame, computeDF: DataFrame, storageDF: DataFrame): DataFrame = {
    // val computeDF = PricingDataLoader(PlatformType.AzureSqlDatabase, "Compute", spark).load()
    // val storageDF = PricingDataLoader(PlatformType.AzureSqlManagedInstance, "Storage", spark).load()
    
    df.transform(SQLDBPricing.transformPlatform())
      .transform(SQLDBPricing.enrichWithStoragePricing(storageDF))
      .transform(SQLDBPricing.enrichWithReservedPricing(computeDF, "1 Year"))
      .transform(SQLDBPricing.enrichWithReservedPricing(computeDF, "3 Years"))
      .transform(SQLDBPricing.addMonthlyCostOptions())      
   }
}
