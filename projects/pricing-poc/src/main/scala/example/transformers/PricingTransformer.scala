package example.transformers

import org.apache.spark.sql.{DataFrame, SparkSession}
import example.pricing._
import example.computations.{
  PricingComputation,
  SqlDbPricingComputation,
  SqlMiPricingComputation,
  SqlVmPricingComputation
}
import example.constants.PlatformType

case class PricingTransformer(platformType: PlatformType, spark: SparkSession)
    extends DataTransformer {
  override def transform(
      df: DataFrame
  ): DataFrame = {
    val computation: PricingComputation = platformType match {
      case PlatformType.AzureSqlDatabase        => new SqlDbPricingComputation(spark)
      case PlatformType.AzureSqlManagedInstance => new SqlMiPricingComputation(spark)
      case PlatformType.AzureSqlVirtualMachine  => new SqlVmPricingComputation(spark)
      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported platform: $platformType"
        )
    }
    computation.compute(df)
  }
}
