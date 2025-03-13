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
  val suitabilityDirPath = Paths.get(reportsDirPath, "suitability").toString

  val spark = SparkUtils.createSparkSession()

  try {
    import spark.implicits._

    // Load JSON files
    val dbFile = Paths.get(skuReportsDirPath, "sku-db.json").toString
    val miFile = Paths.get(skuReportsDirPath, "sku-mi.json").toString
    val vmFile = Paths.get(skuReportsDirPath, "sku-vm.json").toString
    val suitabilityFile = Paths.get(suitabilityDirPath, "suit.json").toString

    val dbDf = JsonReader.readJson(spark, dbFile)
    val miDf = JsonReader.readJson(spark, miFile)
    val vmDf = JsonReader.readJson(spark, vmFile)

    val suitDf = JsonReader.readJson(spark, suitabilityFile)

    dbDf.printSchema()
    miDf.printSchema()
    vmDf.printSchema()
    suitDf.printSchema()

    val suitabilityDf = suitDf
      .withColumn("Servers", explode($"Servers")) // Flatten servers array
      .select(
        $"Servers.TargetReadinesses.AzureSqlDatabase.RecommendationStatus"
          .alias("AzureSqlDatabase_RecommendationStatus"),
        $"Servers.TargetReadinesses.AzureSqlManagedInstance.RecommendationStatus"
          .alias("AzureSqlManagedInstance_RecommendationStatus")
      )

    // Function to transform dataset
    def transformData(
        df: DataFrame,
        suitabilityDf: DataFrame,
        platformName: String
    ): DataFrame = {
      // Map platform names to the extracted recommendation status columns
      val recommendationStatusCol = platformName match {
        case "azureSqlDatabase" => $"AzureSqlDatabase_RecommendationStatus"
        case "azureSqlManagedInstance" =>
          $"AzureSqlManagedInstance_RecommendationStatus"
        case "azureSqlVirtualMachine" =>
          lit("Ready") // Assuming no extracted status for VMs
      }

      // Define whether the extra fields should be included
      val isSqlDbOrMi =
        platformName == "azureSqlDatabase" || platformName == "azureSqlManagedInstance"

      df.withColumn(
        "SkuRecommendationForServers",
        explode($"SkuRecommendationForServers")
      ).withColumn("platform", lit(platformName))
        .filter(
          size($"SkuRecommendationForServers.SkuRecommendationResults") > 0
        )
        .withColumn(
          "targetSku",
          if (isSqlDbOrMi) {
            struct(
              $"SkuRecommendationForServers.SkuRecommendationResults"
                .getItem(0)("TargetSku")("Category")
                .dropFields("SqlTargetPlatform")
                .alias("category"),
              $"SkuRecommendationForServers.SkuRecommendationResults"
                .getItem(0)("TargetSku")
                .getField("StorageMaxSizeInMb")
                .alias("storageMaxSizeInMb"),
              $"SkuRecommendationForServers.SkuRecommendationResults"
                .getItem(0)("TargetSku")
                .getField("PredictedDataSizeInMb")
                .alias("predictedDataSizeInMb"),
              $"SkuRecommendationForServers.SkuRecommendationResults"
                .getItem(0)("TargetSku")
                .getField("PredictedLogSizeInMb")
                .alias("predictedLogSizeInMb"),
              $"SkuRecommendationForServers.SkuRecommendationResults"
                .getItem(0)("TargetSku")
                .getField("MaxStorageIops")
                .alias("maxStorageIops"),
              $"SkuRecommendationForServers.SkuRecommendationResults"
                .getItem(0)("TargetSku")
                .getField("MaxThroughputMBps")
                .alias("maxThroughputMBps"),
              $"SkuRecommendationForServers.SkuRecommendationResults"
                .getItem(0)("TargetSku")("ComputeSize")
                .alias("computeSize")
            )
          } else {
            struct(
              $"SkuRecommendationForServers.SkuRecommendationResults"
                .getItem(0)("TargetSku")("Category")
                .dropFields("SqlTargetPlatform")
                .alias("category"),
              $"SkuRecommendationForServers.SkuRecommendationResults"
                .getItem(0)("TargetSku")("PredictedDataSizeInMb")
                .alias("predictedDataSizeInMb"),
              $"SkuRecommendationForServers.SkuRecommendationResults"
                .getItem(0)("TargetSku")("PredictedLogSizeInMb")
                .alias("predictedLogSizeInMb"),
              $"SkuRecommendationForServers.SkuRecommendationResults"
                .getItem(0)("TargetSku")("VirtualMachineSize")
                .alias("virtualMachineSize"),
              $"SkuRecommendationForServers.SkuRecommendationResults"
                .getItem(0)("TargetSku")("DataDiskSizes")
                .alias("dataDiskSizes"),
              $"SkuRecommendationForServers.SkuRecommendationResults"
                .getItem(0)("TargetSku")("LogDiskSizes")
                .alias("logDiskSizes"),
              $"SkuRecommendationForServers.SkuRecommendationResults"
                .getItem(0)("TargetSku")("TempDbDiskSizes")
                .alias("tempDbDiskSizes"),
              $"SkuRecommendationForServers.SkuRecommendationResults"
                .getItem(0)("TargetSku")("ComputeSize")
                .alias("computeSize")
            )
          }
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
              .getItem(0)("MonthlyCost")("IopsCost")
              .alias("iopsCost"),
            $"SkuRecommendationForServers.SkuRecommendationResults"
              .getItem(0)("MonthlyCost")("TotalCost")
              .alias("totalCost")
          )
        )
        // Join with suitabilityDf to get recommendation status
        .join(suitabilityDf, lit(true), "left")
        .withColumn("recommendationStatus", recommendationStatusCol)
        .select(
          $"platform",
          struct(
            $"recommendationStatus".alias("recommendationStatus"),
            lit(0).alias("numberOfServerBlockerIssues"),
            $"targetSku",
            $"monthlyCost"
          ).alias("recommendation")
        )
        .groupBy("platform")
        .agg(
          first("recommendation").alias("recommendation")
        )
    }

    // Transform each dataset separately
    val dbTransformedDf = transformData(dbDf, suitabilityDf, "azureSqlDatabase")
    val miTransformedDf =
      transformData(miDf, suitabilityDf, "azureSqlManagedInstance")
    val vmTransformedDf =
      transformData(vmDf, suitabilityDf, "azureSqlVirtualMachine")

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
