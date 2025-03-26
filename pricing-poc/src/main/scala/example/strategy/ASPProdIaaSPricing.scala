package example.strategy

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import example.constants.PricingType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import scala.jdk.CollectionConverters._

class ASPProdIaaSPricing extends BaseIaaSPricing {
  override def pricingType: String = PricingType.Consumption.toString

  override def applyAdditionalFilters(df: DataFrame): DataFrame = df.filter(col("savingsPlan").isNotNull)

  override def deriveComputeCost(joinedDF: DataFrame, reservationTerm: String): DataFrame = {
    val explodedDF = joinedDF.withColumn("savingsPlan", explode(col("savingsPlan")))

    val minSavingsPlanDF = explodedDF
      .filter(col("savingsPlan.term") === lit(reservationTerm))
      .orderBy(col("savingsPlan.retailPrice").asc)
      .limit(1)
      .select(col("savingsPlan.retailPrice").alias("minRetailPrice"))

    minSavingsPlanDF.withColumn("computeCost", col("minRetailPrice")).drop("minRetailPrice")
  }
}

