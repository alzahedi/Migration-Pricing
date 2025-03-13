package example

import example.reader.JsonReader
import example.utils.SparkUtils
import example.writer.JsonWriter
import java.nio.file.Paths
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main extends App {

  val reportsDirPath = Paths
    .get(System.getProperty("user.dir"), "src", "main", "resources", "reports")
    .toString
  val skuReportsDirPath = Paths.get(reportsDirPath, "sku").toString

  val spark = SparkUtils.createSparkSession()

  try {
    import spark.implicits._

    // Load JSON files
    val dbFile = Paths.get(skuReportsDirPath, "sku-db.json").toString
    val miFile = Paths.get(skuReportsDirPath, "sku-mi.json").toString
    val vmFile = Paths.get(skuReportsDirPath, "sku-vm.json").toString

    val dbDf = JsonReader.readJson(spark, dbFile)
    val miDf = JsonReader.readJson(spark, miFile)
    val vmDf = JsonReader.readJson(spark, vmFile)

    dbDf.printSchema()
    miDf.printSchema()
    vmDf.printSchema()

    // Function to transform dataset
    def transformData(df: DataFrame, platformName: String): DataFrame = {
      df.withColumn(
        "SkuRecommendationForServers",
        explode($"SkuRecommendationForServers")
      ).withColumn("platform", lit(platformName))
        .filter(
          size($"SkuRecommendationForServers.SkuRecommendationResults") > 0
        ) // Ensure array is non-empty
        .withColumn(
          "targetSku",
          struct(
            $"SkuRecommendationForServers.SkuRecommendationResults"
              .getItem(0)("TargetSku")("Category")
              .dropFields("SqlTargetPlatform")
              .alias("category")
          )
        )
        .withColumn(
          "monthlyCost",
          struct(
            $"SkuRecommendationForServers.SkuRecommendationResults"
              .getItem(0)("MonthlyCost")("ComputeCost")
              .alias("computeCost"),
            $"SkuRecommendationForServers.SkuRecommendationResults"
              .getItem(0)("MonthlyCost")("StorageCost")
              .alias("storageCost"),
            $"SkuRecommendationForServers.SkuRecommendationResults"
              .getItem(0)("MonthlyCost")("TotalCost")
              .alias("totalCost")
          )
        )
        .select(
          $"platform",
          struct(
            lit("ReadyWithConditions").alias("recommendationStatus"),
            lit(0).alias("numberOfServerBlockerIssues"),
            $"targetSku",
            $"monthlyCost"
          ).alias("recommendation")
        )
        .groupBy("platform")
        .agg(
          first("recommendation")
            .alias("recommendation") // Ensure object format, not array
        )
    }

    // Transform each dataset separately
    val dbTransformedDf = transformData(dbDf, "azureSqlDatabase")
    val miTransformedDf = transformData(miDf, "azureSqlManagedInstance")
    val vmTransformedDf = transformData(vmDf, "azureSqlVirtualMachine")

    // Creating a final JSON structure as a map (avoiding union)
    val jsonResultDf = dbTransformedDf
      .unionByName(miTransformedDf, allowMissingColumns = true)
      .unionByName(vmTransformedDf, allowMissingColumns = true)
      .groupBy()
      .agg(
        map_from_entries(
          collect_list(
            struct($"platform", $"recommendation")
          )
        ).alias("skuRecommendationResults")
      )

    val outputPath = Paths
      .get(
        System.getProperty("user.dir"),
        "src",
        "main",
        "resources",
        "output",
        "output.json"
      )
      .toString

    JsonWriter.writeToJsonFile(jsonResultDf, outputPath)

  } finally {
    spark.stop()
  }
}
