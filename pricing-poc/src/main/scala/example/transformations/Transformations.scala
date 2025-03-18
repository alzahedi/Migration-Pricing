package example.transformations

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._

object Transformations {

  def transformSuitability(df: DataFrame): DataFrame = {
    df.withColumn("Servers", explode(col("Servers")))
      .select(
        col("Servers.TargetReadinesses.AzureSqlDatabase.RecommendationStatus")
          .alias("AzureSqlDatabase_RecommendationStatus"),
        col("Servers.TargetReadinesses.AzureSqlManagedInstance.RecommendationStatus")
          .alias("AzureSqlManagedInstance_RecommendationStatus")
      )
  }

  def processSkuData(df: DataFrame, platform: String, suitabilityDf: DataFrame): DataFrame = {
    df.transform(transformSku(platform))
      .transform(joinSuitability(platform, suitabilityDf))
      .transform(selectFields)
      .groupBy("platform")
      .agg(first("recommendation").alias("recommendation"))
  }

  private def transformSku(platform: String)(df: DataFrame): DataFrame = {
    df.withColumn("SkuRecommendationForServers", explode(col("SkuRecommendationForServers")))
      .withColumn("platform", lit(platform))
      .transform(filterSkuRecommendationResults())
      .transform(transformTargetSku(platform))
      .transform(transformMonthlyCost)
  }

  private def filterSkuRecommendationResults()(df: DataFrame): DataFrame = {
    df.filter(size(col("SkuRecommendationForServers.SkuRecommendationResults")) > 0)
  }

  private def transformTargetSku(platform: String)(df: DataFrame): DataFrame = {
    platform match {
      case "azureSqlVirtualMachine" =>
        df.transform(transformComputeSize)
          .transform(transformPredictedDataSize)
          .transform(transformPredictedLogSize)
          .transform(transformCategorySqlVM)
          .transform(transformDiskSize("DataDiskSizes"))
          .transform(transformDiskSize("LogDiskSizes"))
          .transform(transformDiskSize("TempDbDiskSizes"))
          .transform(transformVirtualMachineSize)
          .withColumn("targetSku", struct(col("category"), col("predictedDataSizeInMb"), col("predictedLogSizeInMb"), col("virtualMachineSize"), col("dataDiskSizes"), col("logDiskSizes"), col("tempDbDiskSizes"), col("computeSize")))
      case _ =>
        df.transform(transformComputeSize)
          .transform(transformStorageMaxSize)
          .transform(transformPredictedDataSize)
          .transform(transformPredictedLogSize)
          .transform(transformMaxStorageIops)
          .transform(transformMaxThroughputMBps)
          .transform(transformCategory)
          .withColumn("targetSku", struct(col("category"), col("storageMaxSizeInMb"), col("predictedDataSizeInMb"), col("predictedLogSizeInMb"), col("maxStorageIops"), col("maxThroughputMBps"), col("computeSize")))
    }
  }

  private def joinSuitability(platform: String, suitDf: DataFrame)(df: DataFrame): DataFrame = {
    val recommendationStatusCol = platform match {
      case "azureSqlDatabase" => col("AzureSqlDatabase_RecommendationStatus")
      case "azureSqlManagedInstance" => col("AzureSqlManagedInstance_RecommendationStatus")
      case _ => lit("Ready")
    }
    df.join(suitDf, lit(true), "left")
      .withColumn("recommendationStatus", recommendationStatusCol)
  }

  private def selectFields(df: DataFrame): DataFrame = {
    df.select(
      col("platform"),
      struct(
        col("recommendationStatus").alias("recommendationStatus"),
        lit(0).alias("numberOfServerBlockerIssues"),
        col("targetSku"),
        col("monthlyCost")
      ).alias("recommendation")
    )
  }
  
 private def transformVirtualMachineSize(df: DataFrame): DataFrame = {
    df.withColumn("virtualMachineSize", struct(
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

  private def transformComputeSize(df: DataFrame): DataFrame =
    df.withColumn("computeSize", col("SkuRecommendationForServers.SkuRecommendationResults").getItem(0)("TargetSku")("ComputeSize"))

  private def transformStorageMaxSize(df: DataFrame): DataFrame =
    df.withColumn("storageMaxSizeInMb", col("SkuRecommendationForServers.SkuRecommendationResults").getItem(0)("TargetSku")("StorageMaxSizeInMb"))

  private def transformPredictedDataSize(df: DataFrame): DataFrame =
    df.withColumn("predictedDataSizeInMb", col("SkuRecommendationForServers.SkuRecommendationResults").getItem(0)("TargetSku")("PredictedDataSizeInMb"))

  private def transformPredictedLogSize(df: DataFrame): DataFrame =
    df.withColumn("predictedLogSizeInMb", col("SkuRecommendationForServers.SkuRecommendationResults").getItem(0)("TargetSku")("PredictedLogSizeInMb"))

  private def transformMaxStorageIops(df: DataFrame): DataFrame =
    df.withColumn("maxStorageIops", col("SkuRecommendationForServers.SkuRecommendationResults").getItem(0)("TargetSku")("MaxStorageIops"))

  private def transformMaxThroughputMBps(df: DataFrame): DataFrame =
    df.withColumn("maxThroughputMBps", col("SkuRecommendationForServers.SkuRecommendationResults").getItem(0)("TargetSku")("MaxThroughputMBps"))

  private def transformDiskSize(diskSize: String)(df: DataFrame): DataFrame = {
    val colName = diskSize.head.toLower + diskSize.tail
    df.withColumn(colName, col("SkuRecommendationForServers.SkuRecommendationResults").getItem(0)("TargetSku")(diskSize))
  }

  private def transformCategory(df: DataFrame): DataFrame = {
    df.withColumn("category",
      struct(
        col("SkuRecommendationForServers.SkuRecommendationResults").getItem(0)("TargetSku")("Category")("ComputeTier").alias("computeTier"),
        col("SkuRecommendationForServers.SkuRecommendationResults").getItem(0)("TargetSku")("Category")("HardwareType").alias("hardwareType"),
        col("SkuRecommendationForServers.SkuRecommendationResults").getItem(0)("TargetSku")("Category")("SqlPurchasingModel").alias("sqlPurchasingModel"),
        col("SkuRecommendationForServers.SkuRecommendationResults").getItem(0)("TargetSku")("Category")("SqlServiceTier").alias("sqlServiceTier"),
        col("SkuRecommendationForServers.SkuRecommendationResults").getItem(0)("TargetSku")("Category")("ZoneRedundancyAvailable").alias("zoneRedundancyAvailable")
      )
    )
  }

  private def transformCategorySqlVM(df: DataFrame): DataFrame = {
    df.withColumn("category",
      struct(
        col("SkuRecommendationForServers.SkuRecommendationResults").getItem(0)("TargetSku")("Category")("AvailableVmSkus").alias("availableVmSkus"),
        col("SkuRecommendationForServers.SkuRecommendationResults").getItem(0)("TargetSku")("Category")("VirtualMachineFamily").alias("virtualMachineFamily")
      )
    )
  }

  private def transformMonthlyCost(df: DataFrame): DataFrame =
    df.withColumn("monthlyCost",
      struct(
        col("SkuRecommendationForServers.SkuRecommendationResults").getItem(0)("MonthlyCost")("TotalCost").alias("totalCost")
      )
    )

    def aggregateSkuRecommendations(dbDf: DataFrame, miDf: DataFrame, vmDf: DataFrame): DataFrame = {
      dbDf.unionByName(miDf, allowMissingColumns = true)
        .unionByName(vmDf, allowMissingColumns = true)
        .groupBy()
        .agg(
          map_from_entries(
            collect_list(
              struct(col("platform"), col("recommendation"))
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
  } 
} 
