package example

import example.reader.JsonReader
import example.utils.SparkUtils
import example.writer.JsonWriter
import java.nio.file.Paths
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column

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

    val suitabilityDf = suitDf
      .withColumn("Servers", explode($"Servers")) // Flatten servers array
      .select(
        $"Servers.TargetReadinesses.AzureSqlDatabase.RecommendationStatus"
          .alias("AzureSqlDatabase_RecommendationStatus"),
        $"Servers.TargetReadinesses.AzureSqlManagedInstance.RecommendationStatus"
          .alias("AzureSqlManagedInstance_RecommendationStatus")
      )

    // Break up large monolith into smaller functions

    def transformSqlDb()(df: DataFrame): DataFrame = {
      df.withColumn(
        "SkuRecommendationForServers",
        explode($"SkuRecommendationForServers")
      ).withColumn("platform", lit("azureSqlDatabase"))
        .transform(filterSkuRecommendationResults())
        .transform(transformTargetSku())
        .transform(transformMonthlyCost())
    }

    def transformSqlMi()(df: DataFrame): DataFrame = {
      df.withColumn(
        "SkuRecommendationForServers",
        explode($"SkuRecommendationForServers")
      ).withColumn("platform", lit("azureSqlManagedInstance"))
        .transform(filterSkuRecommendationResults())
        .transform(transformTargetSku())
        .transform(transformMonthlyCost())
    }

    def selectFields()(df: DataFrame): DataFrame = {
      df.select(
        $"platform",
        struct(
          $"recommendationStatus".alias("recommendationStatus"),
          lit(0).alias("numberOfServerBlockerIssues"),
          $"targetSku",
          $"monthlyCost"
        ).alias("recommendation")
      )
    }

    def joinSuitability(platformName: String)(df: DataFrame, suitDf: DataFrame): DataFrame = {
      val recommendationStatusCol = platformName match {
        case "azureSqlDatabase" => $"AzureSqlDatabase_RecommendationStatus"
        case "azureSqlManagedInstance" =>
          $"AzureSqlManagedInstance_RecommendationStatus"
        case "azureSqlVirtualMachine" => lit("Ready")
      }
      df.join(suitDf, lit(true), "left")
        .withColumn("recommendationStatus", recommendationStatusCol)
    }

    def filterSkuRecommendationResults()(df: DataFrame): DataFrame = {
      df.filter(
        size($"SkuRecommendationForServers.SkuRecommendationResults") > 0
      )
    }

    def extractCategory(df: DataFrame): DataFrame = {
      df.withColumn(
        "category",
        struct(
          col("SkuRecommendationForServers.SkuRecommendationResults")
            .getItem(0)("TargetSku")("Category")("ComputeTier")
            .alias("computeTier"),
          col("SkuRecommendationForServers.SkuRecommendationResults")
            .getItem(0)("TargetSku")("Category")("HardwareType")
            .alias("hardwareType"),
          col("SkuRecommendationForServers.SkuRecommendationResults")
            .getItem(0)("TargetSku")("Category")("SqlPurchasingModel")
            .alias("sqlPurchasingModel"),
          col("SkuRecommendationForServers.SkuRecommendationResults")
            .getItem(0)("TargetSku")("Category")("SqlServiceTier")
            .alias("sqlServiceTier"),
          col("SkuRecommendationForServers.SkuRecommendationResults")
            .getItem(0)("TargetSku")("Category")("ZoneRedundancyAvailable")
            .alias("zoneRedundancyAvailable")
        )
      )
    }

    def extractComputeSize(df: DataFrame): DataFrame =
      df.withColumn(
        "computeSize",
        $"SkuRecommendationForServers.SkuRecommendationResults"
          .getItem(0)("TargetSku")("ComputeSize")
      )

    def extractStorageMaxSize(df: DataFrame): DataFrame =
      df.withColumn(
        "storageMaxSizeInMb",
        $"SkuRecommendationForServers.SkuRecommendationResults"
          .getItem(0)("TargetSku")("StorageMaxSizeInMb")
      )

    def extractPredictedDataSize(df: DataFrame): DataFrame =
      df.withColumn(
        "predictedDataSizeInMb",
        $"SkuRecommendationForServers.SkuRecommendationResults"
          .getItem(0)("TargetSku")("PredictedDataSizeInMb")
      )

    def extractPredictedLogSize(df: DataFrame): DataFrame =
      df.withColumn(
        "predictedLogSizeInMb",
        $"SkuRecommendationForServers.SkuRecommendationResults"
          .getItem(0)("TargetSku")("PredictedLogSizeInMb")
      )

    def extractMaxStorageIops(df: DataFrame): DataFrame =
      df.withColumn(
        "maxStorageIops",
        $"SkuRecommendationForServers.SkuRecommendationResults"
          .getItem(0)("TargetSku")("MaxStorageIops")
      )

    def extractMaxThroughputMBps(df: DataFrame): DataFrame =
      df.withColumn(
        "maxThroughputMBps",
        $"SkuRecommendationForServers.SkuRecommendationResults"
          .getItem(0)("TargetSku")("MaxThroughputMBps")
      )


    def transformTargetSku()(df: DataFrame): DataFrame =
      df
        .transform(extractComputeSize)
        .transform(extractStorageMaxSize)
        .transform(extractPredictedDataSize)
        .transform(extractPredictedLogSize)
        .transform(extractMaxStorageIops)
        .transform(extractMaxThroughputMBps)
        .transform(extractCategory)
        .withColumn(
          "targetSku",
          struct(
            $"category",
            $"storageMaxSizeInMb",
            $"predictedDataSizeInMb",
            $"predictedLogSizeInMb",
            $"maxStorageIops",
            $"maxThroughputMBps",
            $"computeSize"
          )
        )

    def transformTargetSkuSqlVm()(df: DataFrame): DataFrame =
      df
        .transform(extractComputeSize)
        .transform(extractPredictedDataSize)
        .transform(extractPredictedLogSize)
        .transform(extractCategorySqlVM)
        .transform(extractDiskSize("DataDiskSizes")(_))
        .transform(extractDiskSize("LogDiskSizes")(_))
        .transform(extractDiskSize("TempDbDiskSizes")(_))
        .transform(extractVirtualMachineSize)
        .withColumn(
          "targetSku",
          struct(
            $"category",
            $"predictedDataSizeInMb",
            $"predictedLogSizeInMb",
            $"virtualMachineSize",
            $"dataDiskSizes",
            $"logDiskSizes",
            $"tempDbDiskSizes",
            $"computeSize"
          )
        )
    
    def extractCategorySqlVM(df: DataFrame): DataFrame = {
      df.withColumn(
        "category",
        struct(
          col("SkuRecommendationForServers.SkuRecommendationResults")
            .getItem(0)("TargetSku")("Category")("AvailableVmSkus")
            .alias("availableVmSkus"),
          col("SkuRecommendationForServers.SkuRecommendationResults")
            .getItem(0)("TargetSku")("Category")("VirtualMachineFamily")
            .alias("virtualMachineFamily")
        )
      )
    }   

    def extractVirtualMachineSize(df: DataFrame): DataFrame = {
      df.withColumn(
        "virtualMachineSize",
        struct(
          col("SkuRecommendationForServers.SkuRecommendationResults")
            .getItem(0)("TargetSku")("VirtualMachineSize")("AzureSkuName")
            .alias("azureSkuName"),
          col("SkuRecommendationForServers.SkuRecommendationResults")
            .getItem(0)("TargetSku")("VirtualMachineSize")("ComputeSize")
            .alias("computeSize"),
          col("SkuRecommendationForServers.SkuRecommendationResults")
            .getItem(0)("TargetSku")("VirtualMachineSize")("MaxNetworkInterfaces")
            .alias("maxNetworkInterfaces"),
          col("SkuRecommendationForServers.SkuRecommendationResults")
            .getItem(0)("TargetSku")("VirtualMachineSize")("SizeName")
            .alias("sizeName"),
          col("SkuRecommendationForServers.SkuRecommendationResults")
            .getItem(0)("TargetSku")("VirtualMachineSize")("VirtualMachineFamily")
            .alias("virtualMachineFamily"),
          col("SkuRecommendationForServers.SkuRecommendationResults")
            .getItem(0)("TargetSku")("VirtualMachineSize")("vCPUsAvailable")
            .alias("vCPUsAvailable")
        )
      )
    }

    def extractDiskSize(diskSize: String)(df: DataFrame): DataFrame = {
      val colName = diskSize.head.toLower + diskSize.tail
      df.withColumn(
        colName, 
        col("SkuRecommendationForServers.SkuRecommendationResults")
          .getItem(0)("TargetSku")(diskSize)
      )
    }


    def transformMonthlyCost()(df: DataFrame): DataFrame =
      df.withColumn(
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

    def transformSqlVm()(df: DataFrame): DataFrame = {
      df.withColumn(
        "SkuRecommendationForServers",
        explode($"SkuRecommendationForServers")
      ).withColumn("platform", lit("azureSqlVirtualMachine"))
        .transform(filterSkuRecommendationResults())
        .transform(transformTargetSkuSqlVm())
        .transform(transformMonthlyCost())
    }

    // Transform each dataset separately
    val dbTransformedDf = dbDf
      .transform(transformSqlDb())
      .transform(joinSuitability("azureSqlDatabase")(_, suitabilityDf))
      .transform(selectFields())
      .groupBy("platform")
      .agg(first("recommendation").alias("recommendation"))

    dbTransformedDf.printSchema()
    dbTransformedDf.show(false)

    val miTransformedDf = miDf
      .transform(transformSqlMi())
      .transform(joinSuitability("azureSqlManagedInstance")(_, suitabilityDf))
      .transform(selectFields())
      .groupBy("platform")
      .agg(first("recommendation").alias("recommendation"))

    miTransformedDf.printSchema()
    miTransformedDf.show(false)

    val vmTransformedDf = vmDf
      .transform(transformSqlVm())
      .transform(joinSuitability("azureSqlVirtualMachine")(_, suitabilityDf))
      .transform(selectFields())
      .groupBy("platform")
      .agg(first("recommendation").alias("recommendation"))

    vmTransformedDf.printSchema()
    vmTransformedDf.show(false)

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

    jsonResultDf.printSchema()
    jsonResultDf.show(false)

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
