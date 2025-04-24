package example

import example.reader.JsonReader
import example.utils.SparkUtils
import example.writer.JsonWriter
import java.nio.file.Paths
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import example.constants.PricingType
import org.apache.spark.sql.Column
import example.constants.PlatformType

object Main extends App {
  val reportsDirPath = Paths
    .get(System.getProperty("user.dir"), "src", "main", "resources", "reports")
    .toString
  val pricingDirPath = Paths
    .get(System.getProperty("user.dir"), "src", "main", "resources", "pricing")
    .toString
  val log4jConfigPath =
    Paths.get(System.getProperty("user.dir"), "log4j2.properties").toString
  System.setProperty("log4j.configurationFile", s"file://$log4jConfigPath")

  implicit val spark = SparkSession
    .builder()
    .appName("poc")
    .master("local[*]")
    .config(
      "spark.sql.streaming.statefulOperator.checkCorrectness.enabled",
      "false"
    )
    .getOrCreate()

  import spark.implicits._
  val skuDbPath = Paths.get(reportsDirPath, "sku", "sku-db.json").toString
  val df = JsonReader(skuDbPath, spark).read()
  //skuDF.printSchema()

import org.apache.spark.sql.functions._
val reservationTerm = "1 Year"
val reservationTermToColName: Map[String, String] = Map(
    "1 Year" -> "1Yr",
    "3 Years" -> "3Yr"
)

val reservationTermToFactor: Map[String, Double] = Map(
    "1 Year" -> 12.0,
    "3 Years" -> 36.0
)

def computeMonthlyCost(priceColumn: Column, months: Double): Column = {
    round(priceColumn / months, 2)
}

 val computeDBPath = Paths.get(pricingDirPath, "SQL_DB_Compute.json").toString
  val rawDF = JsonReader(computeDBPath, spark).read()
  val pricingDf = rawDF.selectExpr("explode(Content) as Content").select("Content.*")

  val storageDBPath = Paths.get(pricingDirPath, "SQL_MI_Storage.json").toString
  val storageRawDF = JsonReader(storageDBPath, spark).read()
  val storageDf = storageRawDF.selectExpr("explode(Content) as Content").select("Content.*")

 val filteredStorageDf = storageDf
    .filter(
      col("location") === "US West" &&
      col("type") === PricingType.Consumption.toString
    )
    .select(col("skuName"), col("retailPrice"))

  val minPricingDf =pricingDf
    .filter(
      col("skuName") === "vCore" &&
        col("location") === "US West" &&
        col("type") === PricingType.Reservation.toString &&
        col("reservationTerm") === reservationTerm &&
        col("UnitOfMeasure") === "1 Hour"
    )
    .withColumn(
      "sqlServiceTier",
      when(col("productName").contains("General Purpose"), "General Purpose")
        .when(
          col("productName").contains("Business Critical"),
          "Business Critical"
        )
        .when(col("productName").contains("Hyperscale"), "Hyperscale")
        .otherwise("Unknown")
    )
    .withColumn(
      "sqlHardwareType",
      when(col("productName").contains("Gen5"), "Gen5")
        .when(
          col("productName").contains("Premium Series Compute"),
          "Premium Series Compute"
        )
        .when(
          col("productName")
            .contains("Premium Series Memory Optimized Compute"),
          "Premium Series Memory Optimized Compute"
        )
        .otherwise("Unknown")
    )
    .groupBy("sqlServiceTier", "sqlHardwareType")
    .agg(min("retailPrice").alias(s"minRetailPrice_$reservationTerm"))

val pricingMapExprEntries = minPricingDf
  .select(
    concat_ws("|", col("sqlServiceTier"), col("sqlHardwareType")).as("key"),
    col(s"minRetailPrice_$reservationTerm").as("value")
  )
  .collect()
  .flatMap(row => Seq(lit(row.getString(0)), lit(row.getDouble(1))))

val pricingExpr = map(pricingMapExprEntries: _*)

val storageMapExprEntries = filteredStorageDf
  .select(
    col("skuName").as("key"),
    col("retailPrice").as("value")
  )
  .collect()
  .flatMap(row => Seq(lit(row.getString(0)), lit(row.getDouble(1))))

val storagePricingExpr = map(storageMapExprEntries: _*)

val updatedDf = df.withColumn(
  "SkuRecommendationForServers",
  transform(col("SkuRecommendationForServers"), server =>
    server.withField(
      "SkuRecommendationResults",
      transform(server.getField("SkuRecommendationResults"), result =>
        result
          .withField(
            "sqlServiceTier",
            when(result.getField("TargetSku").getField("Category").getField("SqlServiceTier")
              .isin("General Purpose", "Next-Gen General Purpose"), "General Purpose")
              .when(result.getField("TargetSku").getField("Category").getField("SqlServiceTier") === "Business Critical", "Business Critical")
              .when(result.getField("TargetSku").getField("Category").getField("SqlServiceTier") === "Hyperscale", "Hyperscale")
              .otherwise("Unknown")
          )
          .withField(
            "sqlHardwareType",
            when(result.getField("TargetSku").getField("Category").getField("HardwareType") === "Gen5", "Gen5")
              .when(result.getField("TargetSku").getField("Category").getField("HardwareType") === "Premium Series", "Premium Series Compute")
              .when(result.getField("TargetSku").getField("Category").getField("HardwareType") === "Premium Series - Memory Optimized", "Premium Series Memory Optimized Compute")
              .otherwise("Unknown")
          )
          .withField(
            "targetPlatform",
            result.getField("TargetSku").getField("Category").getField("SqlTargetPlatform")
          )
          .withField(
            "storageMaxSizeInGb",
            result.getField("TargetSku").getField("StorageMaxSizeInMb") / 1024
          )
          .withField(
            "computeSize",
            result.getField("TargetSku").getField("ComputeSize")
          )
      )
    )
  )
)

val outputDf = updatedDf.withColumn(
  "SkuRecommendationForServers",
  transform(col("SkuRecommendationForServers"), server =>
    server.withField(
      "SkuRecommendationResults",
      transform(server.getField("SkuRecommendationResults"), result =>
        result.withField(
            s"computeCost_${reservationTermToColName(reservationTerm)}",
            computeMonthlyCost(result.getField("computeSize") *
            element_at(
              pricingExpr,
              concat_ws("|", result.getField("sqlServiceTier"), result.getField("sqlHardwareType"))
            ),
            reservationTermToFactor(reservationTerm)
            )
        )
       .withField(
            "storageCost",
            bround(
              when(
                result.getField("targetPlatform") === PlatformType.AzureSqlManagedInstance.toString,
                element_at(storagePricingExpr, result.getField("sqlServiceTier")) *
                  greatest(result.getField("storageMaxSizeInGb") - 32, lit(0))
              ).otherwise(
                element_at(storagePricingExpr, result.getField("sqlServiceTier")) *
                  (result.getField("storageMaxSizeInGb") * 1.3)
              ),
              2
            )
        )
      )
    )))

  val resultDf = outputDf.withColumn("SkuRecommendationForServers", explode(col("SkuRecommendationForServers")))
      .withColumn("SkuRecommendationResults", explode(col("SkuRecommendationForServers.SkuRecommendationResults")))
      .select("SkuRecommendationResults")
  outputDf.printSchema()
 
 outputDf.select("TimeCreated").show(false)
 
  val selectedDf = resultDf.select(
  col("SkuRecommendationResults.DatabaseName").as("DatabaseName"),
  col("SkuRecommendationResults.computeCost_1Yr").as("ComputeCost1yr"),
  col("SkuRecommendationResults.storageCost").as("storageCost")
)

// .withColumn(s"computeCost_${reservationTermToColName(reservationTerm)}",
//         computeMonthlyCost(
//           col("computeSize") * col(s"minRetailPrice_$reservationTerm"),
//           reservationTermToFactor(reservationTerm)
//         )
//       )

selectedDf.show(false)

}
