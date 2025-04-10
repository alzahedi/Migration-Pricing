// package example.strategy


// import org.apache.spark.sql.DataFrame
// import org.apache.spark.sql.types._
// import org.apache.spark.sql.functions._
// import example.constants.PricingType
// import org.apache.spark.sql.SparkSession
// import org.apache.spark.sql.Row
// import example.constants.ReservationTermToNumMap

// class ReservedProdIaaSPricing(val reservationTerm: String) extends BaseIaaSPricing {
//   override def pricingType: String = PricingType.Reservation.toString

//   override def applyAdditionalFilters(df: DataFrame): DataFrame = {
//     df.filter(col("reservationTerm").isNotNull && col("reservationTerm") === reservationTerm)
//   } 

//   override def deriveComputeCost(joinedDF: DataFrame): DataFrame = {
//     val minRetailPriceDF = joinedDF
//       .orderBy(col(s"retailPrice").asc)
//       .limit(1)
//       .select(col(s"retailPrice").alias("computeCost"))

//     calculateMonthlyCost(minRetailPriceDF, "computeCost", 12 * ReservationTermToNumMap.map.getOrElse(reservationTerm, 0).toString.toDouble, _ / _)
//   }
// }
