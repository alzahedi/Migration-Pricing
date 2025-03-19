package example.calculations

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object PricingComputations {

  private val schema: StructType = StructType(
    Seq(
      StructField("keyName", StringType, nullable = false),
      StructField("keyValue", StructType(
          Seq(
            StructField("computeCost", DoubleType, nullable = false),
            StructField("storageCost", DoubleType, nullable = false),
            StructField("iopsCost", DoubleType, nullable = false)
          )
        ),
        nullable = false
      )
    )
  )

  private val pricingData: Seq[(String, (Double, Double, Double))] = Seq(
    ("With1YearRIAndDevTest", (245.13, 0.18, 0.0)),
    ("With3YearRIAndDevTest", (24.13, 0.18, 0.0)),
    ("With1YearRIAndProd", (242.13, 0.18, 0.0)),
    ("With3YearRIAndProd", (222.13, 0.18, 0.0))
  )

  private val pricingDataForSqlVM: Seq[(String, (Double, Double, Double))] = Seq(
    ("With1YearASPAndDevTest", (245.13, 0.18, 0.0)),
    ("With3YearASPAndDevTest", (24.13, 0.18, 0.0)),
    ("With1YearASPAndProd", (242.13, 0.18, 0.0)),
    ("With3YearASPAndProd", (222.13, 0.18, 0.0)),
    ("With1YearRIAndDevTest", (245.13, 0.18, 0.0)),
    ("With3YearRIAndDevTest", (24.13, 0.18, 0.0)),
    ("With1YearRIAndProd", (242.13, 0.18, 0.0)),
    ("With3YearRIAndProd", (222.13, 0.18, 0.0))
  )

  private def computePricing(df: DataFrame, pricingData: Seq[(String, (Double, Double, Double))])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val data = pricingData.toDF("keyName", "keyValue")

    data.withColumn(
      "keyValue", struct(
        col("keyValue._1").as("computeCost"),
        col("keyValue._2").as("storageCost"),
        col("keyValue._3").as("iopsCost")
      )
    ).select("keyName", "keyValue")
  }

  def computePricingForSqlDB(df: DataFrame): DataFrame = {
    implicit val spark: SparkSession = df.sparkSession
    computePricing(df, pricingData)
  }

  def computePricingForSqlMI(df: DataFrame): DataFrame = {
    implicit val spark: SparkSession = df.sparkSession
    computePricing(df, pricingData)
  }

  def computePricingForSqlVM(df: DataFrame): DataFrame = {
    implicit val spark: SparkSession = df.sparkSession
    computePricing(df, pricingDataForSqlVM)
  }

}
