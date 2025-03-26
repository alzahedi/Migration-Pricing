package example.strategy

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import example.constants.PricingType

class ASPDevTestIaaSPricing extends BaseIaaSPricing {
  override def pricingType: String = PricingType.DevTestConsumption.toString

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


