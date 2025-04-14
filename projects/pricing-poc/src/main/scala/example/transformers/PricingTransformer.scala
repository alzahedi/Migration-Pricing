package example.transformers

import org.apache.spark.sql.{DataFrame, SparkSession}
import example.pricing._
import example.computations.{PricingComputation, SqlDbPricingComputation, SqlMiPricingComputation, SqlVmPricingComputation}
import example.constants.PlatformType

case class PricingTransformer(platformType: PlatformType) extends Transformer {
  override def transform(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val computation: PricingComputation = platformType match {
      case PlatformType.AzureSqlDatabase         => new SqlDbPricingComputation
      case PlatformType.AzureSqlManagedInstance  => new SqlMiPricingComputation
      case PlatformType.AzureSqlVirtualMachine   => new SqlVmPricingComputation
      case _ => throw new UnsupportedOperationException(s"Unsupported platform: $platformType")
    }
    computation.compute(df)
  }
}

