package example.calculations

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import example.reader.JsonReader
import example.constants.PlatformType
import java.nio.file.Paths
import example.calculator.{PaasPricingCalculator,PricingCalculator}
import example.strategy.PricingStrategyFactory

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

  private def generatePricingValues(
      platformDf: DataFrame,
      computePricingDf: DataFrame,
      storagePricingDf: DataFrame,
      reservationTerm: String,
      environment: String,
      pricingModel: String,
      pricingType: String
  ): (Double, Double, Double) = {

    // Select the right strategy dynamically
    val strategy = PricingStrategyFactory.getStrategy(pricingModel, pricingType)

    // Compute cost
    val computeCost = strategy.computeCost(platformDf, computePricingDf, reservationTerm)
    val storageCost = strategy.storageCost(platformDf, storagePricingDf)
    val iopsCost = 0.0

    (computeCost, storageCost, iopsCost)
  }



  private def structurePricingData(df: DataFrame, pricingData: Seq[(String, (Double, Double, Double))])(implicit spark: SparkSession): DataFrame = {
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
    val pricingDataFrames = loadPricingDataFrames(PlatformType.AzureSqlDatabase)
    val computeDataFrame = pricingDataFrames.get("Compute").getOrElse(throw new RuntimeException(s"Compute pricing data not found"))
    val storageDataFrame = pricingDataFrames.get("Storage").getOrElse(throw new RuntimeException(s"Storage pricing data not found"))
    val pricingCalculator = new PaasPricingCalculator
    
    val pricingData: Seq[(String, (Double, Double, Double))] = Seq(
      ("With1YearRIAndDevTest", (245.13, 0.18, 0.0)),
      ("With3YearRIAndDevTest", (24.13, 0.18, 0.0)),
      ("With1YearRIAndProd", generatePricingValues(df, computeDataFrame, storageDataFrame, "1 Year", "Prod", "PaaS", "RI")),
      ("With3YearRIAndProd", generatePricingValues(df, computeDataFrame, storageDataFrame, "3 Years", "Prod", "PaaS", "RI"))
    )

    structurePricingData(df, pricingData)
  }


  def computePricingForSqlMI(df: DataFrame): DataFrame = {
    implicit val spark: SparkSession = df.sparkSession
    val pricingDataFrames = loadPricingDataFrames(PlatformType.AzureSqlManagedInstance)
    val computeDataFrame = pricingDataFrames.get("Compute").getOrElse(throw new RuntimeException(s"Compute pricing data not found"))
    val storageDataFrame = pricingDataFrames.get("Storage").getOrElse(throw new RuntimeException(s"Storage pricing data not found"))
    val pricingCalculator = new PaasPricingCalculator
    val pricingData: Seq[(String, (Double, Double, Double))] = Seq(
      ("With1YearRIAndDevTest", (245.13, 0.18, 0.0)),
      ("With3YearRIAndDevTest", (24.13, 0.18, 0.0)),
      ("With1YearRIAndProd", generatePricingValues(df, computeDataFrame, storageDataFrame, "1 Year", "Prod", "PaaS", "RI")),
      ("With3YearRIAndProd", generatePricingValues(df, computeDataFrame, storageDataFrame, "3 Years", "Prod", "PaaS", "RI"))
    )
    structurePricingData(df, pricingData)
  }

  def computePricingForSqlVM(df: DataFrame): DataFrame = {
    implicit val spark: SparkSession = df.sparkSession
    structurePricingData(df, pricingDataForSqlVM)
  }

  def loadPricingDataFrames(platformType: PlatformType)(implicit spark: SparkSession): Map[String, DataFrame] = {
    val fileMappings: Map[PlatformType, Map[String, String]] = Map(
      PlatformType.AzureSqlDatabase -> Map(
        "Compute" -> "SQL_DB_Compute.json",
        // "License" -> "SQL_DB_License.json",
        "Storage" -> "SQL_DB_Storage.json"
      ),
      PlatformType.AzureSqlManagedInstance -> Map(
        "Compute" -> "SQL_MI_Compute.json",
        // "License" -> "SQL_MI_License.json",
        "Storage" -> "SQL_MI_Storage.json"
      ),
      PlatformType.AzureSqlVirtualMachine -> Map(
        "Compute" -> "SQL_VM_Compute.json",
        "Storage" -> "SQL_VM_Storage.json"
      )
    )

    val pricingDataFolderPath = Paths.get(System.getProperty("user.dir"), "src", "main", "resources", "pricing").toString
    val fileMapping = fileMappings.getOrElse(platformType, Map.empty)

    fileMapping.map { case (category, fileName) =>
      val filePath = s"$pricingDataFolderPath/$fileName"
      category -> JsonReader.readJson(spark, filePath)
    }
  }
}
