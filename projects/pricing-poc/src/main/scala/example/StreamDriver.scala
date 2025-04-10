import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.eventhubs._
import com.azure.identity.AzureCliCredentialBuilder
import com.azure.core.credential.TokenRequestContext
import org.apache.spark.eventhubs.utils.AadAuthenticationCallback
import java.util.concurrent.CompletableFuture
import java.nio.file.Paths
import org.apache.spark.sql.streaming.Trigger
import example.reader.JsonReader
import example.constants.{MigrationAssessmentSourceTypes, MigrationAssessmentConstants}
import example.transformations.TransformationsV1
import example.constants.PlatformType
import example.computations.PricingComputationsV1

object StreamDriver extends App {

  val reportsDirPath = Paths.get(System.getProperty("user.dir"), "src", "main", "resources", "reports").toString
  val log4jConfigPath = Paths.get(System.getProperty("user.dir"), "log4j2.properties").toString
  System.setProperty("log4j.configurationFile", s"file://$log4jConfigPath")

  val spark = SparkSession.builder()
    .appName("EventHubReader")
    .master("local[*]") 
    .getOrCreate()

  import spark.implicits._

  val eventHubNamespace = "pricing-streaming"
  val eventHubName = "streaming-input"
  val outputEventHubName = "streaming-output"

  val suitabilityConsumerGroup = "suitability-instance-spark"
  val skuDbConsumerGroup = "sku-sql-db-instance-spark"
  val skuMiConsumerGroup = "sku-sql-mi-instance-spark"
  val skuVmConsumerGroup = "sku-sql-vm-instance-spark"

  // Azure CLI Authentication
  val credential = new AzureCliCredentialBuilder().build()
  val tokenContext = new TokenRequestContext().addScopes("https://eventhubs.azure.net/.default")

  val aadAuthCallback = new AadAuthenticationCallback {
    override def authority: String = "https://login.microsoftonline.com/"
    override def acquireToken(audience: String, authority: String, state: Any): CompletableFuture[String] = {
      CompletableFuture.supplyAsync(() => credential.getToken(tokenContext).block().getToken)
    }
  }
  
  def createEventHubConf(consumerGroup: String): EventHubsConf = {
    EventHubsConf(s"Endpoint=sb://$eventHubNamespace.servicebus.windows.net/;EntityPath=$eventHubName")
      .setAadAuthCallback(aadAuthCallback)
      .setConsumerGroup(consumerGroup)
      .setStartingPosition(EventPosition.fromStartOfStream)
  }

  val outputEventHubConf = EventHubsConf(s"Endpoint=sb://$eventHubNamespace.servicebus.windows.net/;EntityPath=$outputEventHubName")
    .setAadAuthCallback(aadAuthCallback)

  val suitabilityEventHubsConf = createEventHubConf(suitabilityConsumerGroup)
  val skuDbEventHubsConf = createEventHubConf(skuDbConsumerGroup)
  val skuMiEventHubsConf = createEventHubConf(skuMiConsumerGroup)
  val skuVmEventHubsConf = createEventHubConf(skuVmConsumerGroup)

  val suitabilityEventStream = spark.readStream
  .format("eventhubs")
  .options(suitabilityEventHubsConf.toMap)
  .load()

  val skuDbEventStream = spark.readStream
    .format("eventhubs")
    .options(skuDbEventHubsConf.toMap)
    .load()

  val skuMiEventStream = spark.readStream
    .format("eventhubs")
    .options(skuMiEventHubsConf.toMap)
    .load()

  val skuVmEventStream = spark.readStream
    .format("eventhubs")
    .options(skuVmEventHubsConf.toMap)
    .load()

  val eventSchema = StructType(Seq(
    StructField("uploadIdentifier", StringType, nullable = false),
    StructField("type", StringType, nullable = false),
    StructField("body", StringType, nullable = false)
  ))

  val schemaStructNames: Map[MigrationAssessmentSourceTypes.Value, String] = Map(
      MigrationAssessmentSourceTypes.Suitability -> "suitability_report_struct",
      MigrationAssessmentSourceTypes.SkuRecommendationDB -> "sku_recommendation_azuresqldb_sku_recommendation_report_struct",
      MigrationAssessmentSourceTypes.SkuRecommendationMI -> "sku_recommendation_azuresqlmi_sku_recommendation_report_struct",
      MigrationAssessmentSourceTypes.SkuRecommendationVM -> "sku_recommendation_azuresqlvm_sku_recommendation_report_struct"
    )


  def parseStream(eventStream: DataFrame): DataFrame = {
    eventStream
      .selectExpr("CAST(body AS STRING) AS message", "enqueuedTime")
      .select(from_json($"message", eventSchema).as("data"), $"enqueuedTime")
      .select("data.*", "enqueuedTime")
      .withColumn("timestamp", current_timestamp())
  }

  val suitabilityParsedStream = parseStream(suitabilityEventStream)
  val skuDbParsedStream = parseStream(skuDbEventStream)
  val skuMiParsedStream = parseStream(skuMiEventStream)
  val skuVmParsedStream = parseStream(skuVmEventStream)

  val skuDbProcessStream = processStream(skuDbParsedStream, MigrationAssessmentSourceTypes.SkuRecommendationDB)
  val skuMiProcessStream = processStream(skuMiParsedStream, MigrationAssessmentSourceTypes.SkuRecommendationMI)

  //val dbPricingData = PricingComputationsV1.computePricingForSqlDB(skuDbProcessStream)
  val miPricingData = PricingComputationsV1.computePricingForSqlMI(skuMiProcessStream)

  // val suitDF = processStream(suitabilityParsedStream, MigrationAssessmentSourceTypes.Suitability)
  //               .transform(TransformationsV1.transformSuitability)
  
  // val skuDbDF = processStream(skuDbParsedStream, MigrationAssessmentSourceTypes.SkuRecommendationDB)
  //               .transform(TransformationsV1.processSkuData(PlatformType.AzureSqlDatabase, 
  //                schemaStructNames(MigrationAssessmentSourceTypes.SkuRecommendationDB)))

  // val skuMiDF = processStream(skuMiParsedStream, MigrationAssessmentSourceTypes.SkuRecommendationMI)
  //               .transform(TransformationsV1.processSkuData(PlatformType.AzureSqlManagedInstance,
  //                schemaStructNames(MigrationAssessmentSourceTypes.SkuRecommendationMI)))

  // val skuVmDF = processStream(skuVmParsedStream, MigrationAssessmentSourceTypes.SkuRecommendationVM)
  //               .transform(TransformationsV1.processSkuData(PlatformType.AzureSqlVirtualMachine,
  //               schemaStructNames(MigrationAssessmentSourceTypes.SkuRecommendationVM)))

  // suitDF.printSchema()
  // skuDbDF.printSchema()
  // skuMiDF.printSchema()
  // skuVmDF.printSchema()

  // spark.conf.set("spark.sql.streaming.join.debug", "true")
  // val joinedDF = suitDF.as("es")
  //                 .join(skuDbDF.as("esrasd"), expr(s"""(es.uploadIdentifier == esrasd.uploadIdentifier) AND (es.enqueuedTime BETWEEN (esrasd.enqueuedTime - ${MigrationAssessmentConstants.DefaultAcrossStreamsIntervalMaxLag}) AND (esrasd.enqueuedTime + ${MigrationAssessmentConstants.DefaultAcrossStreamsIntervalMaxLag}))"""), joinType = "inner")
  //                 .join(skuMiDF.as("esrasm"), expr(s"""(es.uploadIdentifier == esrasm.uploadIdentifier) AND (es.enqueuedTime BETWEEN (esrasm.enqueuedTime - ${MigrationAssessmentConstants.DefaultAcrossStreamsIntervalMaxLag}) AND (esrasm.enqueuedTime + ${MigrationAssessmentConstants.DefaultAcrossStreamsIntervalMaxLag}))"""), joinType = "inner")
  //                 .join(skuVmDF.as("esrasv"), expr(s"""(es.uploadIdentifier == esrasv.uploadIdentifier) AND (es.enqueuedTime BETWEEN (esrasv.enqueuedTime - ${MigrationAssessmentConstants.DefaultAcrossStreamsIntervalMaxLag}) AND (esrasv.enqueuedTime + ${MigrationAssessmentConstants.DefaultAcrossStreamsIntervalMaxLag}))"""), joinType = "inner")
  //                 .drop("type")
  //                 .drop("timestamp")
  //                 .drop("uploadIdentifier")
  //                 //.drop("enqueuedTime")

  // //joinedDF.printSchema()
  // val outputDF = processInstanceUpdateEventStream(joinedDF)
  miPricingData.printSchema()

  // Process the result, e.g., showing it or saving to a file
  val query = miPricingData.writeStream
    .outputMode("append")
    .option("checkpointLocation", "/workspaces/Migration-Pricing/projects/pricing-poc/src/main/resources/output/checkpoint")
    .trigger(Trigger.ProcessingTime("60 seconds")).queryName("myTable")
    .format("memory")
    .start()

  while(true) {
    println("Checking data.....")
    Thread.sleep(1000)
    spark.sql("SELECT computeCost_1Yr, computeCost_3Yr, storageCost, monthlyCostOptions FROM myTable").show(10000, true)
  }

  // val serializedDF = outputDF
  // .select(to_json(struct("*")).cast("string").alias("body"))
  // .selectExpr("CAST(body AS BINARY) AS body")

  // val serializedDF = outputDF
  //   .select(to_json(struct("*")).alias("body"))
  //   .selectExpr("CAST(body AS BINARY) AS body")

  // serializedDF.printSchema()

  // println("Starting write to event hub....")
  // val query = serializedDF
  //   .writeStream
  //   .outputMode("append")
  //   .format("eventhubs")
  //   .options(outputEventHubConf.toMap)
  //   .option("checkpointLocation", "/workspaces/Migration-Pricing/projects/pricing-poc/src/main/resources/output/eventhub-checkpoint") // Must be a reliable location like DBFS or HDFS
  //   .start()
  //   .awaitTermination()

  def processStream(inDF: DataFrame, maType: MigrationAssessmentSourceTypes.Value): DataFrame = {
    val jsonPaths: Map[MigrationAssessmentSourceTypes.Value, String] = Map(
      MigrationAssessmentSourceTypes.Suitability -> Paths.get(reportsDirPath, "suitability", "suit.json").toString,
      MigrationAssessmentSourceTypes.SkuRecommendationDB -> Paths.get(reportsDirPath, "sku", "sku-db.json").toString,
      MigrationAssessmentSourceTypes.SkuRecommendationMI -> Paths.get(reportsDirPath, "sku", "sku-mi.json").toString,
      MigrationAssessmentSourceTypes.SkuRecommendationVM -> Paths.get(reportsDirPath, "sku", "sku-vm.json").toString
    )
    val schema = JsonReader.readJson(spark, jsonPaths(maType)).schema

    inDF.filter($"type" === maType.toString)
        .select(col("*"), from_json(col("body"), schema).as("body_struct"))
        .drop("body")
        .select(col("*"), col("body_struct.*"))
        .drop("body_struct")
        .withWatermark("enqueuedTime", MigrationAssessmentConstants.DefaultLateArrivingWatermarkTime)
  }

  def processInstanceUpdateEventStream(
      inDF: DataFrame
  ): DataFrame = {
    inDF.withColumn("assessmentUploadTime", col("es.enqueuedTime"))
        .withColumn("serverAssessments", col("suitability_report_struct.ServerAssessments"))
        .withColumn("azuresqlvm_skuRecommendationResults", col("sku_recommendation_azuresqlvm_sku_recommendation_report_struct"))
        .withColumn("azuresqldb_skuRecommendationResults", col("sku_recommendation_azuresqldb_sku_recommendation_report_struct"))
        .withColumn("azuresqlmi_skuRecommendationResults", col("sku_recommendation_azuresqlmi_sku_recommendation_report_struct"))
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
                                        col("suitability_report_struct.AzureSqlDatabase_RecommendationStatus").alias("recommendationStatus"),
                                        col("suitability_report_struct.AzureSqlDatabase_NumberOfServerBlockerIssues").alias("numberOfServerBlockerIssues"),
                                        //element_at(col("suitability_report_struct.Servers.TargetReadinesses.AzureSqlDatabase.NumberOfServerBlockerIssues"), 1).alias("numberOfServerBlockerIssues"),
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
                                    ).alias("azureSqlDatabase"),
                                    struct(
                                        col("suitability_report_struct.AzureSqlManagedInstance_RecommendationStatus").alias("recommendationStatus"),
                                        col("suitability_report_struct.AzureSqlManagedInstance_NumberOfServerBlockerIssues").alias("numberOfServerBlockerIssues"),
                                        //element_at(col("suitability_report_struct.Servers.TargetReadinesses.AzureSqlManagedInstance.NumberOfServerBlockerIssues"), 1).alias("numberOfServerBlockerIssues"),
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
                                    ).alias("azureSqlVirtualMachine")
                                ).alias("skuRecommendationResults")
                            ).alias("assessment")
                        ).alias("migration")
                    ).alias("properties")
              )
            )
          )   
        .select("arm_resource")
    
    // updatedDF.printSchema()
    // updatedDF
  }
}
