package example.transformers

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import example.constants.MigrationAssessmentSourceTypes
import example.constants.MigrationAssessmentConstants
import example.reader.JsonReader
import java.nio.file.Paths
import example.constants.PlatformType
import example.computations.{
  PricingComputation,
  SqlDbPricingComputation,
  SqlMiPricingComputation,
  SqlVmPricingComputation
}

class MigrationAssessmentTransformer(
    resourceType: MigrationAssessmentSourceTypes.Value,
    spark: SparkSession,
    platformType: PlatformType = null,
    skuDbDF: DataFrame = null,
    skuMiDF: DataFrame = null,
    skuVmDF: DataFrame = null
) extends DataTransformer {

  override def transform(df: DataFrame): DataFrame = {
    resourceType match {
      case MigrationAssessmentSourceTypes.EventHubRawEventStream => processRawEventHubStream(df)
      case MigrationAssessmentSourceTypes.Suitability         |
           MigrationAssessmentSourceTypes.SkuRecommendationDB |
           MigrationAssessmentSourceTypes.SkuRecommendationMI | 
           MigrationAssessmentSourceTypes.SkuRecommendationVM    => processTypedEventHubStream(df)
      case MigrationAssessmentSourceTypes.PricingComputation     => processPricingComputation(df)
      case MigrationAssessmentSourceTypes.FullAssessment         => processFullAssessment(df)
      case MigrationAssessmentSourceTypes.InstanceUpdate         => processInstanceUpdate(df)
    }
  }

  private def processFullAssessment(df: DataFrame): DataFrame = {
    df.as("es")
      .join(skuDbDF.as("esrasd"), expr(s"""(es.uploadIdentifier == esrasd.uploadIdentifier) AND (es.enqueuedTime BETWEEN (esrasd.enqueuedTime - ${MigrationAssessmentConstants.DefaultAcrossStreamsIntervalMaxLag}) AND (esrasd.enqueuedTime + ${MigrationAssessmentConstants.DefaultAcrossStreamsIntervalMaxLag}))"""), joinType = "inner")
      .join(skuMiDF.as("esrasm"), expr(s"""(es.uploadIdentifier == esrasm.uploadIdentifier) AND (es.enqueuedTime BETWEEN (esrasm.enqueuedTime - ${MigrationAssessmentConstants.DefaultAcrossStreamsIntervalMaxLag}) AND (esrasm.enqueuedTime + ${MigrationAssessmentConstants.DefaultAcrossStreamsIntervalMaxLag}))"""), joinType = "inner")
      .join(skuVmDF.as("esrasv"), expr(s"""(es.uploadIdentifier == esrasv.uploadIdentifier) AND (es.enqueuedTime BETWEEN (esrasv.enqueuedTime - ${MigrationAssessmentConstants.DefaultAcrossStreamsIntervalMaxLag}) AND (esrasv.enqueuedTime + ${MigrationAssessmentConstants.DefaultAcrossStreamsIntervalMaxLag}))"""), joinType = "inner")
      .drop("type")
      .drop("timestamp")
      .drop("uploadIdentifier")
  }

  private def processInstanceUpdate(
      df: DataFrame
  ): DataFrame = {
    df.withColumn("assessmentUploadTime", col("es.enqueuedTime"))
        .withColumn("serverAssessments", col("es.Servers.ServerAssessments"))
        .withColumn("azuresqlvm_skuRecommendationForServers", col("esrasv.SkuRecommendationForServers"))
        .withColumn("azuresqldb_skuRecommendationForServers", col("esrasd.SkuRecommendationForServers"))
        .withColumn("azuresqlmi_skuRecommendationForServers", col("esrasm.SkuRecommendationForServers"))
        .withColumn("azuresqlvm_skuRecommendationResults", element_at(col("azuresqlvm_skuRecommendationForServers.SkuRecommendationResults"), 1))
        .withColumn("azuresqldb_skuRecommendationResults", element_at(col("azuresqldb_skuRecommendationForServers.SkuRecommendationResults"), 1))
        .withColumn("azuresqlmi_skuRecommendationResults", element_at(col("azuresqlmi_skuRecommendationForServers.SkuRecommendationResults"), 1))
        .withColumn("arm_resource",
          to_json(
                  struct(
                    struct(
                        struct(
                            struct(
                                col("assessmentUploadTime"),
                                col("serverAssessments"),
                                struct(
                                    struct(
                                      element_at(col("es.Servers.TargetReadinesses.AzureSqlDatabase.RecommendationStatus"), 1).alias("recommendationStatus"),
                                      element_at(col("es.Servers.TargetReadinesses.AzureSqlDatabase.NumberOfServerBlockerIssues"), 1).alias("numberOfServerBlockerIssues"),
                                        struct(
                                            struct(
                                                col("azuresqldb_skuRecommendationResults.TargetSku.Category.ComputeTier").alias("computeTier"),
                                                col("azuresqldb_skuRecommendationResults.TargetSku.Category.HardwareType").alias("hardwareType"),
                                                col("azuresqldb_skuRecommendationResults.TargetSku.Category.SqlPurchasingModel").alias("sqlPurchasingModel"),
                                                col("azuresqldb_skuRecommendationResults.TargetSku.Category.SqlServiceTier").alias("sqlServiceTier"),
                                                col("azuresqldb_skuRecommendationResults.TargetSku.Category.ZoneRedundancyAvailable").alias("zoneRedundancyAvailable")
                                            ).alias("category"),
                                            col("azuresqldb_skuRecommendationResults.TargetSku.storageMaxSizeInMb").alias("storageMaxSizeInMb"),
                                            col("azuresqldb_skuRecommendationResults.TargetSku.predictedDataSizeInMb").alias("predictedDataSizeInMb"),
                                            col("azuresqldb_skuRecommendationResults.TargetSku.predictedLogSizeInMb").alias("predictedLogSizeInMb"),
                                            col("azuresqldb_skuRecommendationResults.TargetSku.maxStorageIops").alias("maxStorageIops"),
                                            col("azuresqldb_skuRecommendationResults.TargetSku.maxThroughputMBps").alias("maxThroughputMBps"),
                                            col("azuresqldb_skuRecommendationResults.TargetSku.computeSize").alias("computeSize")
                                        ).alias("targetSku"),
                                        struct(
                                            col("azuresqldb_skuRecommendationResults.MonthlyCost.ComputeCost").alias("computeCost"),
                                            col("azuresqldb_skuRecommendationResults.MonthlyCost.StorageCost").alias("storageCost"),
                                            col("azuresqldb_skuRecommendationResults.MonthlyCost.iopsCost").alias("iopsCost"),
                                            col("azuresqldb_skuRecommendationResults.MonthlyCost.TotalCost").alias("totalCost")
                                        ).alias("monthlyCost"),
                                        col("esrasd.monthlyCostOptions").alias("monthlyCostOptions"),
                                    ).alias("azureSqlDatabase"),
                                    struct(
                                        element_at(col("es.Servers.TargetReadinesses.AzureSqlManagedInstance.RecommendationStatus"), 1).alias("recommendationStatus"),
                                        element_at(col("es.Servers.TargetReadinesses.AzureSqlManagedInstance.NumberOfServerBlockerIssues"), 1).alias("numberOfServerBlockerIssues"),
                                        struct(
                                            struct(
                                                col("azuresqlmi_skuRecommendationResults.TargetSku.Category.ComputeTier").alias("computeTier"),
                                                col("azuresqlmi_skuRecommendationResults.TargetSku.Category.HardwareType").alias("hardwareType"),
                                                col("azuresqlmi_skuRecommendationResults.TargetSku.Category.SqlPurchasingModel").alias("sqlPurchasingModel"),
                                                col("azuresqlmi_skuRecommendationResults.TargetSku.Category.SqlServiceTier").alias("sqlServiceTier"),
                                                col("azuresqlmi_skuRecommendationResults.TargetSku.Category.ZoneRedundancyAvailable").alias("zoneRedundancyAvailable")
                                            ).alias("category"),
                                            col("azuresqlmi_skuRecommendationResults.TargetSku.storageMaxSizeInMb").alias("storageMaxSizeInMb"),
                                            col("azuresqlmi_skuRecommendationResults.TargetSku.predictedDataSizeInMb").alias("predictedDataSizeInMb"),
                                            col("azuresqlmi_skuRecommendationResults.TargetSku.predictedLogSizeInMb").alias("predictedLogSizeInMb"),
                                            col("azuresqlmi_skuRecommendationResults.TargetSku.maxStorageIops").alias("maxStorageIops"),
                                            col("azuresqlmi_skuRecommendationResults.TargetSku.maxThroughputMBps").alias("maxThroughputMBps"),
                                            col("azuresqlmi_skuRecommendationResults.TargetSku.computeSize").alias("computeSize")
                                        ).alias("targetSku"),
                                        struct(
                                            col("azuresqlmi_skuRecommendationResults.MonthlyCost.ComputeCost").alias("computeCost"),
                                            col("azuresqlmi_skuRecommendationResults.MonthlyCost.StorageCost").alias("storageCost"),
                                            col("azuresqlmi_skuRecommendationResults.MonthlyCost.iopsCost").alias("iopsCost"),
                                            col("azuresqlmi_skuRecommendationResults.MonthlyCost.TotalCost").alias("totalCost")
                                        ).alias("monthlyCost"),
                                        col("esrasm.monthlyCostOptions").alias("monthlyCostOptions"),
                                    ).alias("azureSqlManagedInstance"),
                                    struct(
                                        lit("Ready").alias("recommendationStatus"),
                                        lit(0).alias("numberOfServerBlockerIssues"),
                                        struct(
                                            struct(
                                                col("azuresqlvm_skuRecommendationResults.TargetSku.Category.AvailableVmSkus").alias("availableVmSkus"),
                                                col("azuresqlvm_skuRecommendationResults.TargetSku.Category.VirtualMachineFamily").alias("virtualMachineFamily"),
                                            ).alias("category"),
                                            col("azuresqlvm_skuRecommendationResults.TargetSku.predictedDataSizeInMb").alias("predictedDataSizeInMb"),
                                            col("azuresqlvm_skuRecommendationResults.TargetSku.predictedLogSizeInMb").alias("predictedLogSizeInMb"),
                                            struct(
                                              col("azuresqlvm_skuRecommendationResults.TargetSku.virtualMachineSize.azureSkuName").alias("azureSkuName"),
                                              col("azuresqlvm_skuRecommendationResults.TargetSku.virtualMachineSize.computeSize").alias("computeSize"),
                                              col("azuresqlvm_skuRecommendationResults.TargetSku.virtualMachineSize.maxNetworkInterfaces").alias("maxNetworkInterfaces"),
                                              col("azuresqlvm_skuRecommendationResults.TargetSku.virtualMachineSize.sizeName").alias("sizeName"),
                                              col("azuresqlvm_skuRecommendationResults.TargetSku.virtualMachineSize.virtualMachineFamily").alias("virtualMachineFamily"),
                                              col("azuresqlvm_skuRecommendationResults.TargetSku.virtualMachineSize.vCPUsAvailable").alias("vCPUsAvailable"),
                                            ).alias("virtualMachineSize"),
                                            col("azuresqlvm_skuRecommendationResults.TargetSku.dataDiskSizes").alias("dataDiskSizes"),
                                            col("azuresqlvm_skuRecommendationResults.TargetSku.logDiskSizes").alias("logDiskSizes"),
                                            col("azuresqlvm_skuRecommendationResults.TargetSku.tempDbDiskSizes").alias("tempDbDiskSizes"),
                                            col("azuresqlvm_skuRecommendationResults.TargetSku.computeSize").alias("computeSize")
                                        ).alias("targetSku"),
                                        struct(
                                            col("azuresqlvm_skuRecommendationResults.MonthlyCost.ComputeCost").alias("computeCost"),
                                            col("azuresqlvm_skuRecommendationResults.MonthlyCost.StorageCost").alias("storageCost"),
                                            col("azuresqlvm_skuRecommendationResults.MonthlyCost.iopsCost").alias("iopsCost"),
                                            col("azuresqlvm_skuRecommendationResults.MonthlyCost.TotalCost").alias("totalCost")
                                        ).alias("monthlyCost"),
                                        col("esrasv.monthlyCostOptions").alias("monthlyCostOptions"),
                                    ).alias("azureSqlVirtualMachine")
                                ).alias("skuRecommendationResults")
                            ).alias("assessment")
                        ).alias("migration")
                    ).alias("properties")
              )
            )
          )   
        .select("arm_resource")
  }

  private def processRawEventHubStream(df: DataFrame): DataFrame = {
    val eventSchema = StructType(Seq(
      StructField("uploadIdentifier", StringType, nullable = false),
      StructField("type", StringType, nullable = false),
      StructField("body", StringType, nullable = false)
    ))

    df
      .selectExpr("CAST(body AS STRING) AS message", "enqueuedTime")
      .select(from_json(col("message"), eventSchema).as("data"), col("enqueuedTime"))
      .select("data.*", "enqueuedTime")
      .withColumn("timestamp", current_timestamp())
  }

  private def processPricingComputation(df: DataFrame): DataFrame = {
    val computation: PricingComputation = platformType match {
      case PlatformType.AzureSqlDatabase        => new SqlDbPricingComputation(spark)
      case PlatformType.AzureSqlManagedInstance => new SqlMiPricingComputation(spark)
      case PlatformType.AzureSqlVirtualMachine  => new SqlVmPricingComputation(spark)
      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported platform: $platformType"
        )
    }
    computation.compute(df)
  }

  private def processTypedEventHubStream(df: DataFrame): DataFrame = {
    val reportsDirPath = Paths.get(System.getProperty("user.dir"), "src", "main", "resources", "reports").toString
    val jsonPaths: Map[MigrationAssessmentSourceTypes.Value, String] = Map(
      MigrationAssessmentSourceTypes.Suitability -> Paths.get(reportsDirPath, "suitability", "suit.json").toString,
      MigrationAssessmentSourceTypes.SkuRecommendationDB -> Paths.get(reportsDirPath, "sku", "sku-db.json").toString,
      MigrationAssessmentSourceTypes.SkuRecommendationMI -> Paths.get(reportsDirPath, "sku", "sku-mi.json").toString,
      MigrationAssessmentSourceTypes.SkuRecommendationVM -> Paths.get(reportsDirPath, "sku", "sku-vm.json").toString
    )
    val schema = JsonReader(jsonPaths(resourceType), spark).read().schema

    df.filter(col("type") === resourceType.toString)
        .select(col("*"), from_json(col("body"), schema).as("body_struct"))
        .drop("body")
        .select(col("*"), col("body_struct.*"))
        .drop("body_struct")
        .withWatermark("enqueuedTime", MigrationAssessmentConstants.DefaultLateArrivingWatermarkTime)
  }
}

object MigrationAssessmentTransformer {
  def apply(
    resourceType: MigrationAssessmentSourceTypes.Value, 
    spark: SparkSession, 
    platformType: PlatformType = null,
    skuDbDF: DataFrame = null,
    skuMiDF: DataFrame = null,
    skuVmDF: DataFrame = null
): MigrationAssessmentTransformer = {
    new MigrationAssessmentTransformer(resourceType, spark, platformType, skuDbDF, skuMiDF, skuVmDF)
  }
}
