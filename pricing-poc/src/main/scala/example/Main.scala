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

  val reportsDirPath = Paths.get(System.getProperty("user.dir"), "src", "main", "resources", "reports").toString
  val log4jConfigPath = Paths.get(System.getProperty("user.dir"), "log4j2.properties").toString
  System.setProperty("log4j.configurationFile", s"file://$log4jConfigPath")

  val skuReportsDirPath = Paths.get(reportsDirPath, "sku").toString
  val suitabilityDirPath = Paths.get(reportsDirPath, "suitability").toString
  val sampleReportsDirPath = Paths.get(reportsDirPath, "samples").toString

  val spark = SparkUtils.createSparkSession()

  try {
    import spark.implicits._

    // Load JSON files
    val dbFile = Paths.get(skuReportsDirPath, "sku-db.json").toString
    val miFile = Paths.get(skuReportsDirPath, "sku-mi.json").toString
    val vmFile = Paths.get(skuReportsDirPath, "sku-vm.json").toString
    val suitabilityFile = Paths.get(suitabilityDirPath, "suit.json").toString

    val vmSchemaFile = Paths.get(sampleReportsDirPath, "sku-vm-full-fidelity.json").toString

    val dbDf = JsonReader.readJson(spark, dbFile)
    val miDf = JsonReader.readJson(spark, miFile)
    val vmDf = JsonReader.readJsonWithSchemaInferred(spark, vmFile, vmSchemaFile)

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

    def transformCategory(df: DataFrame): DataFrame = {
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

    def transformComputeSize(df: DataFrame): DataFrame =
      df.withColumn(
        "computeSize",
        $"SkuRecommendationForServers.SkuRecommendationResults"
          .getItem(0)("TargetSku")("ComputeSize")
      )

    def transformStorageMaxSize(df: DataFrame): DataFrame =
      df.withColumn(
        "storageMaxSizeInMb",
        $"SkuRecommendationForServers.SkuRecommendationResults"
          .getItem(0)("TargetSku")("StorageMaxSizeInMb")
      )

    def transformPredictedDataSize(df: DataFrame): DataFrame =
      df.withColumn(
        "predictedDataSizeInMb",
        $"SkuRecommendationForServers.SkuRecommendationResults"
          .getItem(0)("TargetSku")("PredictedDataSizeInMb")
      )

    def transformPredictedLogSize(df: DataFrame): DataFrame =
      df.withColumn(
        "predictedLogSizeInMb",
        $"SkuRecommendationForServers.SkuRecommendationResults"
          .getItem(0)("TargetSku")("PredictedLogSizeInMb")
      )

    def transformMaxStorageIops(df: DataFrame): DataFrame =
      df.withColumn(
        "maxStorageIops",
        $"SkuRecommendationForServers.SkuRecommendationResults"
          .getItem(0)("TargetSku")("MaxStorageIops")
      )

    def transformMaxThroughputMBps(df: DataFrame): DataFrame =
      df.withColumn(
        "maxThroughputMBps",
        $"SkuRecommendationForServers.SkuRecommendationResults"
          .getItem(0)("TargetSku")("MaxThroughputMBps")
      )


    def transformTargetSku()(df: DataFrame): DataFrame =
      df
        .transform(transformComputeSize)
        .transform(transformStorageMaxSize)
        .transform(transformPredictedDataSize)
        .transform(transformPredictedLogSize)
        .transform(transformMaxStorageIops)
        .transform(transformMaxThroughputMBps)
        .transform(transformCategory)
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
        .transform(transformComputeSize)
        .transform(transformPredictedDataSize)
        .transform(transformPredictedLogSize)
        .transform(transformCategorySqlVM)
        .transform(transformDiskSize("DataDiskSizes")(_))
        .transform(transformDiskSize("LogDiskSizes")(_))
        .transform(transformDiskSize("TempDbDiskSizes")(_))
        .transform(transformVirtualMachineSize)
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
    
    def transformCategorySqlVM(df: DataFrame): DataFrame = {
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

    def transformVirtualMachineSize(df: DataFrame): DataFrame = {
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

    def transformDiskSize(diskSize: String)(df: DataFrame): DataFrame = {
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
      .withColumn("skuRecommendationResults",
        expr(
          """
          transform(
            map_values(skuRecommendationResults),
            x -> struct(
              x.recommendationStatus as recommendationStatus,
              x.numberOfServerBlockerIssues as numberOfServerBlockerIssues,
              struct(
                x.targetSku.category as category,
                x.targetSku.storageMaxSizeInMb as storageMaxSizeInMb,
                x.targetSku.predictedDataSizeInMb as predictedDataSizeInMb,
                x.targetSku.predictedLogSizeInMb as predictedLogSizeInMb,
                x.targetSku.maxStorageIops as maxStorageIops,
                x.targetSku.maxThroughputMBps as maxThroughputMBps,
                x.targetSku.computeSize as computeSize,
                transform(
                  x.targetSku.dataDiskSizes,
                  y -> struct(
                    y.Caching as caching,
                    y.MaxIOPS as maxIOPS,
                    y.MaxSizeInGib as maxSizeInGib,
                    y.MaxThroughputInMbps as maxThroughputInMbps,
                    y.Redundancy as redundancy,
                    y.Size as size,
                    y.Type as type
                  )
                ) as dataDiskSizes,
                transform(
                  x.targetSku.logDiskSizes,
                  y -> struct(
                    y.Caching as caching,
                    y.MaxIOPS as maxIOPS,
                    y.MaxSizeInGib as maxSizeInGib,
                    y.MaxThroughputInMbps as maxThroughputInMbps,
                    y.Redundancy as redundancy,
                    y.Size as size,
                    y.Type as type
                  )
                ) as logDiskSizes,
                transform(
                  x.targetSku.tempDbDiskSizes,
                  y -> struct(
                    y.Caching as caching,
                    y.MaxIOPS as maxIOPS,
                    y.MaxSizeInGib as maxSizeInGib,
                    y.MaxThroughputInMbps as maxThroughputInMbps,
                    y.Redundancy as redundancy,
                    y.Size as size,
                    y.Type as type
                  )
                ) as tempDbDiskSizes,
                x.targetSku.virtualMachineSize as virtualMachineSize
              ) as targetSku,
              x.monthlyCost as monthlyCost
            )
          )
          """
        )
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
