package example.transformations


import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._
import example.constants.PlatformType

object TransformationsV1 {

  def transformSuitability(df: DataFrame): DataFrame = {
    df.withColumn("Servers", explode(col("Servers")))
        .withColumn(
          "suitability_report_struct",
          struct(
            col("Servers.ServerAssessments").alias("ServerAssessments"),
            col("Servers.TargetReadinesses.AzureSqlDatabase.RecommendationStatus")
              .alias("AzureSqlDatabase_RecommendationStatus"),
            col("Servers.TargetReadinesses.AzureSqlDatabase.NumberOfServerBlockerIssues")
              .alias("AzureSqlDatabase_NumberOfServerBlockerIssues"),
            col("Servers.TargetReadinesses.AzureSqlManagedInstance.RecommendationStatus")
              .alias("AzureSqlManagedInstance_RecommendationStatus"),
            col("Servers.TargetReadinesses.AzureSqlManagedInstance.NumberOfServerBlockerIssues")
              .alias("AzureSqlManagedInstance_NumberOfServerBlockerIssues")
          )
        )
      .select(
        col("uploadIdentifier"),
        col("type"),
        col("timestamp"),
        col("enqueuedTime"),
        col("suitability_report_struct"))
  }

  def processSkuData(platform: PlatformType, payloadColName: String)(df: DataFrame): DataFrame = {
    df.transform(transformSku(platform))
      .transform(selectFields(payloadColName))
    //   .transform(joinSuitability(platform, suitabilityDf))
    //   .transform(selectFields)
    //   .groupBy("platform")
    //   .agg(first("recommendation").alias("recommendation"))
  }

  private def transformSku(platform: PlatformType)(df: DataFrame): DataFrame = {
    //df.withColumn("SkuRecommendationForServers", explode(col("SkuRecommendationForServers")))
    df.transform(filterSkuRecommendationResults())
      .transform(transformTargetSku(platform))
      .transform(transformMonthlyCost)
  }

  private def filterSkuRecommendationResults()(df: DataFrame): DataFrame = {
    df.filter(size(col("SkuRecommendationForServers.SkuRecommendationResults")) > 0)
  }

  private def transformTargetSku(platform: PlatformType)(df: DataFrame): DataFrame = {
    platform match {
      case PlatformType.AzureSqlVirtualMachine =>
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

  private def joinSuitability(platform: PlatformType, suitDf: DataFrame)(df: DataFrame): DataFrame = {
    val recommendationStatusCol = platform match {
      case PlatformType.AzureSqlDatabase         => col("AzureSqlDatabase_RecommendationStatus")
      case PlatformType.AzureSqlManagedInstance  => col("AzureSqlManagedInstance_RecommendationStatus")
      case _                                     => lit("Ready")
    }

    df.join(suitDf, lit(true), "left")
      .withColumn("recommendationStatus", recommendationStatusCol)
  }

  private def selectFields(payloadColName: String)(df: DataFrame): DataFrame = {
    df.select(
      col("uploadIdentifier"),
      col("timestamp"),
      col("enqueuedTime"),
      struct(
        col("targetSku"),
        col("monthlyCost"),
        col("monthlyCostOptions")
      ).alias(payloadColName)
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
        col("SkuRecommendationForServers.SkuRecommendationResults").getItem(0)("MonthlyCost")("ComputeCost").alias("computeCost"),
        col("SkuRecommendationForServers.SkuRecommendationResults").getItem(0)("MonthlyCost")("StorageCost").alias("storageCost"),
        col("SkuRecommendationForServers.SkuRecommendationResults").getItem(0)("MonthlyCost")("IopsCost").alias("iopsCost"),
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
          map_from_entries(
            transform(
              map_entries(skuRecommendationResults),
              entry -> struct(
                entry.key as platformName,
                struct(
                  entry.value.recommendationStatus as recommendationStatus,
                  entry.value.numberOfServerBlockerIssues as numberOfServerBlockerIssues,
                  struct(
                    entry.value.targetSku.category as category,  -- Retain full category
                    entry.value.targetSku.storageMaxSizeInMb as storageMaxSizeInMb,
                    entry.value.targetSku.predictedDataSizeInMb as predictedDataSizeInMb,
                    entry.value.targetSku.predictedLogSizeInMb as predictedLogSizeInMb,
                    entry.value.targetSku.maxStorageIops as maxStorageIops,
                    entry.value.targetSku.maxThroughputMBps as maxThroughputMBps,
                    entry.value.targetSku.computeSize as computeSize,
                    transform(
                      entry.value.targetSku.dataDiskSizes,
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
                      entry.value.targetSku.logDiskSizes,
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
                      entry.value.targetSku.tempDbDiskSizes,
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
                    entry.value.targetSku.virtualMachineSize as virtualMachineSize
                  ) as targetSku,
                  struct(
                    entry.value.monthlyCost.computeCost as computeCost,
                    entry.value.monthlyCost.storageCost as storageCost,
                    entry.value.monthlyCost.iopsCost as iopsCost,
                    entry.value.monthlyCost.totalCost as totalCost
                  ) as monthlyCost,
                  transform(
                    entry.value.monthlyCostOptions,
                    y -> struct(
                      y.keyName as keyName,
                      struct(
                        y.keyValue.computeCost as computeCost,
                        y.keyValue.storageCost as storageCost,
                        y.keyValue.iopsCost as iopsCost
                      ) as keyValue
                    )
                  ) as monthlyCostOptions
                )
              )
            )
          )
          """
        )
      )
    } 

  def mergePricingIntoRecommendation(df: DataFrame, pricingDf: DataFrame): DataFrame = {
    val pricingArray = pricingDf
        .agg(collect_list(struct("keyName", "keyValue")).as("monthlyCostOptions"))

    // Perform cross join to add the array to all rows without dropping it
    val mergedDf = df.crossJoin(pricingArray) // Add monthlyCostOptions to all rows
        .withColumn(
            "recommendation",
            struct(
                col("recommendation.*"), // Keep existing fields inside recommendation
                col("monthlyCostOptions") // Add the new field inside recommendation
            )
        )
        .drop("monthlyCostOptions")

    mergedDf
  }
  }   
