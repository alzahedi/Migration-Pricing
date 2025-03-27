package example.strategy

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import example.constants.PricingType

class ASPDevTestIaaSPricing extends BaseIaaSPricing {
  override def pricingType: String = PricingType.DevTestConsumption.toString

  override def applyAdditionalFilters(df: DataFrame, reservationTerm: String): DataFrame = {
    val filteredDF = df.filter(col("savingsPlan").isNotNull)
    val explodedDF = filteredDF.withColumn("savingsPlan", explode(col("savingsPlan")))

    explodedDF.filter(col("savingsPlan.term") === lit(reservationTerm))
  }


  override def deriveComputeCost(joinedDF: DataFrame, reservationTerm: String): DataFrame = {
    val minSavingsPlanDF = joinedDF
      .orderBy(col("savingsPlan.retailPrice").asc)
      .limit(1)
      .select(col("savingsPlan.retailPrice").alias("minRetailPrice"))

    val resultDF = minSavingsPlanDF.withColumn("computeCost", col("minRetailPrice")).drop("minRetailPrice")
    calculateMonthlyCost(resultDF, "computeCost", 24 * 30.5, _ * _)
  }
}


