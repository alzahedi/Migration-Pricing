package example.strategy


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import example.constants.PricingType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import scala.jdk.CollectionConverters._

class ReservedProdIaaSPricing extends BaseIaaSPricing {
  override def pricingType: String = PricingType.Reservation.toString

  override def applyAdditionalFilters(df: DataFrame): DataFrame = df.filter(col("reservationTerm").isNotNull)

  override def deriveComputeCost(joinedDF: DataFrame, reservationTerm: String): DataFrame = {
    val minRetailPriceDF = joinedDF
      .orderBy(col(s"retailPrice").asc)
      .limit(1)
      .select(col(s"retailPrice").alias("computeCost"))

    minRetailPriceDF
  }
}
