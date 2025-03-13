package example

import example.reader.JsonReader
import example.utils.SparkUtils
import example.writer.JsonWriter
import java.nio.file.Paths
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

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
      val recommendationStatusCol = platformName match {
        case "azureSqlDatabase" => $"AzureSqlDatabase_RecommendationStatus"
        case "azureSqlManagedInstance" =>
          $"AzureSqlManagedInstance_RecommendationStatus"
        case "azureSqlVirtualMachine" => lit("Ready") // Default for VMs
      }

      val isSqlDbOrMi =
        platformName == "azureSqlDatabase" || platformName == "azureSqlManagedInstance"

// Define the schema for the array of structs
      val structSchema = ArrayType(
        StructType(
          Seq(
            StructField("Caching", StringType),
            StructField("MaxIOPS", IntegerType),
            StructField("MaxSizeInGib", IntegerType),
            StructField("MaxThroughputInMbps", IntegerType),
            StructField("Redundancy", StringType),
            StructField("Size", IntegerType),
            StructField("Type", StringType)
          )
        )
      )

// Define an empty array with the correct schema
      val emptyStructArray = lit(null).cast(
        structSchema
      ) // Ensures an empty array with the correct type

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
              struct(
                $"SkuRecommendationForServers.SkuRecommendationResults"
                  .getItem(0)("TargetSku")("Category")("ComputeTier")
                  .alias("computeTier"),
                $"SkuRecommendationForServers.SkuRecommendationResults"
                  .getItem(0)("TargetSku")("Category")("HardwareType")
                  .alias("hardwareType"),
                $"SkuRecommendationForServers.SkuRecommendationResults"
                  .getItem(0)("TargetSku")("Category")("SqlPurchasingModel")
                  .alias("sqlPurchasingModel"),
                $"SkuRecommendationForServers.SkuRecommendationResults"
                  .getItem(0)("TargetSku")("Category")("SqlServiceTier")
                  .alias("sqlServiceTier"),
                $"SkuRecommendationForServers.SkuRecommendationResults"
                  .getItem(0)("TargetSku")("Category")(
                    "ZoneRedundancyAvailable"
                  )
                  .alias("zoneRedundancyAvailable")
              ).alias("category"),
              $"SkuRecommendationForServers.SkuRecommendationResults"
                .getItem(0)("TargetSku")("StorageMaxSizeInMb")
                .alias("storageMaxSizeInMb"),
              $"SkuRecommendationForServers.SkuRecommendationResults"
                .getItem(0)("TargetSku")("PredictedDataSizeInMb")
                .alias("predictedDataSizeInMb"),
              $"SkuRecommendationForServers.SkuRecommendationResults"
                .getItem(0)("TargetSku")("PredictedLogSizeInMb")
                .alias("predictedLogSizeInMb"),
              $"SkuRecommendationForServers.SkuRecommendationResults"
                .getItem(0)("TargetSku")("MaxStorageIops")
                .alias("maxStorageIops"),
              $"SkuRecommendationForServers.SkuRecommendationResults"
                .getItem(0)("TargetSku")("MaxThroughputMBps")
                .alias("maxThroughputMBps"),
              $"SkuRecommendationForServers.SkuRecommendationResults"
                .getItem(0)("TargetSku")("ComputeSize")
                .alias("computeSize")
            )
          } else {
            struct(
              struct(
                $"SkuRecommendationForServers.SkuRecommendationResults"
                  .getItem(0)("TargetSku")("Category")("AvailableVmSkus")
                  .alias("availableVmSkus"),
                $"SkuRecommendationForServers.SkuRecommendationResults"
                  .getItem(0)("TargetSku")("Category")("VirtualMachineFamily")
                  .alias("virtualMachineFamily")
              ).alias("category"),
              $"SkuRecommendationForServers.SkuRecommendationResults"
                .getItem(0)("TargetSku")("PredictedDataSizeInMb")
                .alias("predictedDataSizeInMb"),
              $"SkuRecommendationForServers.SkuRecommendationResults"
                .getItem(0)("TargetSku")("PredictedLogSizeInMb")
                .alias("predictedLogSizeInMb"),
              struct(
                $"SkuRecommendationForServers.SkuRecommendationResults"
                  .getItem(0)("TargetSku")("VirtualMachineSize")("AzureSkuName")
                  .alias("azureSkuName"),
                $"SkuRecommendationForServers.SkuRecommendationResults"
                  .getItem(0)("TargetSku")("VirtualMachineSize")("ComputeSize")
                  .alias("computeSize"),
                $"SkuRecommendationForServers.SkuRecommendationResults"
                  .getItem(0)("TargetSku")("VirtualMachineSize")(
                    "MaxNetworkInterfaces"
                  )
                  .alias("maxNetworkInterfaces"),
                $"SkuRecommendationForServers.SkuRecommendationResults"
                  .getItem(0)("TargetSku")("VirtualMachineSize")("SizeName")
                  .alias("sizeName"),
                $"SkuRecommendationForServers.SkuRecommendationResults"
                  .getItem(0)("TargetSku")("VirtualMachineSize")(
                    "VirtualMachineFamily"
                  )
                  .alias("virtualMachineFamily"),
                $"SkuRecommendationForServers.SkuRecommendationResults"
                  .getItem(0)("TargetSku")("VirtualMachineSize")(
                    "vCPUsAvailable"
                  )
                  .alias("vCPUsAvailable")
              ).alias("virtualMachineSize"),
              $"SkuRecommendationForServers.SkuRecommendationResults"
                .getItem(0)("TargetSku")("DataDiskSizes")
                .alias("dataDiskSizes"),
              $"SkuRecommendationForServers.SkuRecommendationResults"
                .getItem(0)("TargetSku")("LogDiskSizes")
                .alias("logDiskSizes"),
              when(
                col("SkuRecommendationForServers.SkuRecommendationResults")(0)(
                  "TargetSku"
                )("TempDbDiskSizes").isNotNull &&
                  size(
                    col("SkuRecommendationForServers.SkuRecommendationResults")(
                      0
                    )(
                      "TargetSku"
                    )("TempDbDiskSizes")
                  ) > 0,
                expr(
                  "transform(SkuRecommendationForServers.SkuRecommendationResults[0].TargetSku.TempDbDiskSizes, x -> struct(" +
                    "x.Caching as caching, x.MaxIOPS as maxIOPS, x.MaxSizeInGib as maxSizeInGib, " +
                    "x.MaxThroughputInMbps as maxThroughputInMbps, x.Redundancy as redundancy, " +
                    "x.Size as size, x.Type as type))"
                )
              ).otherwise(emptyStructArray).alias("tempDbDiskSizes"),
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
        .agg(first("recommendation").alias("recommendation"))
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
