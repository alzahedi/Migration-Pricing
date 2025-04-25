package example

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.eventhubs._
import java.nio.file.Paths
import org.apache.spark.sql.streaming.Trigger
import example.reader.JsonReader
import example.constants.{MigrationAssessmentSourceTypes, MigrationAssessmentConstants}
import example.constants.PlatformType
import example.eventhub.EventHubEntraAuthCallback
import example.eventhub.ReadEventHubFeedFormat
import example.eventhub.EventHubConnection
import example.security.TokenCredentialProvider
import example.eventhub.EventHubStreamFeed
import example.reader.MigrationAssessmentReader
import example.transformers.MigrationAssessmentTransformer
import example.eventhub.WriteEventHubFeedFormat
import example.loader.PricingDataLoader

object StreamDriver extends App {

  val reportsDirPath = Paths.get(System.getProperty("user.dir"), "src", "main", "resources", "reports").toString
  val log4jConfigPath = Paths.get(System.getProperty("user.dir"), "log4j2.properties").toString
  System.setProperty("log4j.configurationFile", s"file://$log4jConfigPath")

  implicit val spark = SparkSession.builder()
    .appName("EventHubReader")
    .master("local[*]") 
    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  val eventHubNamespace = "pricing-streaming"
  val sourceEventHubName = "streaming-input"
  val sinkEventHubName = "streaming-output"
  val eventHubCheckPointLocation = "/workspaces/Migration-Pricing/projects/pricing-poc/src/main/resources/output/eventhub-checkpoint"

  val suitabilityConsumerGroup = "suitability-instance-spark"
  val skuDbConsumerGroup = "sku-sql-db-instance-spark"
  val skuMiConsumerGroup = "sku-sql-mi-instance-spark"
  val skuVmConsumerGroup = "sku-sql-vm-instance-spark"

  // Create auth callback
  val authCallback = EventHubEntraAuthCallback(TokenCredentialProvider.getAzureCliCredential)
  val sourceEventHubConnection = EventHubConnection(eventHubNamespace, sourceEventHubName)
  val sinkEventHubConnection = EventHubConnection(eventHubNamespace, sinkEventHubName)
  
  val suitabilityEventHubFeed = ReadEventHubFeedFormat(
                                  eventHubConnection = sourceEventHubConnection, 
                                  specifiedConsumerGroup = suitabilityConsumerGroup, 
                                  entraCallback = authCallback
                                )

  val skuDbEventHubFeed = ReadEventHubFeedFormat(
                            eventHubConnection = sourceEventHubConnection,
                            specifiedConsumerGroup = skuDbConsumerGroup,
                            entraCallback = authCallback
                          )

  val skuMiEventHubFeed = ReadEventHubFeedFormat(
                            eventHubConnection = sourceEventHubConnection,
                            specifiedConsumerGroup = skuMiConsumerGroup,
                            entraCallback = authCallback
                          )

  val skuVmEventHubFeed = ReadEventHubFeedFormat(
                            eventHubConnection = sourceEventHubConnection,
                            specifiedConsumerGroup = skuVmConsumerGroup,
                            entraCallback = authCallback
                          )

  val computeDBDF   = PricingDataLoader(PlatformType.AzureSqlDatabase, "Compute", spark).load()
  val storagePaasDF = PricingDataLoader(PlatformType.AzureSqlManagedInstance, "Storage", spark).load()

  val computeMIDF = PricingDataLoader(PlatformType.AzureSqlManagedInstance, "Compute", spark).load()

  val computeVMDF = PricingDataLoader(PlatformType.AzureSqlVirtualMachine, "Compute", spark).load()
  val storageVMDF = PricingDataLoader(PlatformType.AzureSqlVirtualMachine, "Storage", spark).load()

  val suitabilityEventStream = MigrationAssessmentReader(spark, suitabilityEventHubFeed, MigrationAssessmentSourceTypes.EventHubRawEventStream).read()
  val skuDbEventStream =  MigrationAssessmentReader(spark, skuDbEventHubFeed, MigrationAssessmentSourceTypes.EventHubRawEventStream).read()
  val skuMiEventStream =  MigrationAssessmentReader(spark, skuMiEventHubFeed, MigrationAssessmentSourceTypes.EventHubRawEventStream).read()
  val skuVmEventStream =  MigrationAssessmentReader(spark, skuVmEventHubFeed, MigrationAssessmentSourceTypes.EventHubRawEventStream).read()

  val suitDF = suitabilityEventStream
    .transform(MigrationAssessmentTransformer(MigrationAssessmentSourceTypes.EventHubRawEventStream, spark).transform)
    .transform(MigrationAssessmentTransformer(MigrationAssessmentSourceTypes.Suitability, spark).transform)
      
  val skuDbDF = skuDbEventStream
    .transform(MigrationAssessmentTransformer(MigrationAssessmentSourceTypes.EventHubRawEventStream, spark).transform)
    .transform(MigrationAssessmentTransformer(MigrationAssessmentSourceTypes.SkuRecommendationDB, spark).transform)
    .transform(MigrationAssessmentTransformer(MigrationAssessmentSourceTypes.PricingComputation, spark, PlatformType.AzureSqlDatabase, computeDF=computeDBDF, storageDF=storagePaasDF).transform)

  // val skuVmDF = skuVmEventStream
  //   .transform(MigrationAssessmentTransformer(MigrationAssessmentSourceTypes.EventHubRawEventStream, spark).transform)
  //   .transform(MigrationAssessmentTransformer(MigrationAssessmentSourceTypes.SkuRecommendationVM, spark).transform)
  //   .transform(MigrationAssessmentTransformer(MigrationAssessmentSourceTypes.PricingComputation, spark, PlatformType.AzureSqlVirtualMachine, computeDF=computeVMDF, storageDF=storageVMDF).transform)
  

  // skuVmDF.printSchema()

  // val resultDf = skuVmDF.withColumn("SkuRecommendationForServers", explode(col("SkuRecommendationForServers")))
  //     .withColumn("SkuRecommendationResults", explode(col("SkuRecommendationForServers.SkuRecommendationResults")))
  //     .select("SkuRecommendationResults", "uploadIdentifier")
 
 
//   val selectedDf = resultDf.select(
//   col("SkuRecommendationResults.computeCostRI_1Year").as("ComputeCost1yr"),
//   col("SkuRecommendationResults.computeCostRI_3Years").as("ComputeCost3yr"),
//   col("SkuRecommendationResults.computeCostASPProd_1Year"),
//   col("SkuRecommendationResults.computeCostASPProd_3Years"),
//   col("SkuRecommendationResults.computeCostASPDevTest_1Year"),
//   col("SkuRecommendationResults.computeCostASPDevTest_3Years")
//   // col("SkuRecommendationResults.storageCost").as("storageCost"),
//   // col("SkuRecommendationResults.monthlyCostOptions").as("monthlyCostOptions"),
// )

//   val selectedDf = resultDf.select(
//   col("uploadIdentifier"),
//   //col("SkuRecommendationResults.DatabaseName").as("databaseName"),
//   //col("SkuRecommendationResults.storageCost").as("storageCost"),
//   col("SkuRecommendationResults.monthlyCostOptions").as("monthlyCostOptions"),
// )

// //   Process the result, e.g., showing it or saving to a file
//   val query = selectedDf.writeStream
//     .outputMode("append")
//     .option("checkpointLocation", "/workspaces/Migration-Pricing/projects/pricing-poc/src/main/resources/output/checkpoint")
//     .trigger(Trigger.ProcessingTime("60 seconds")).queryName("myTable")
//     .format("memory")
//     .start()

//   while(true) {
//     println("Checking data.....")
//     Thread.sleep(1000)
//     //spark.sql("SELECT computeCost_1Yr, computeCost_3Yr, monthlyCostOptions FROM myTable").show(10000, true)
//     spark.sql("SELECT * FROM myTable").show(10000, false)  
//   }
  val skuMiDF = skuMiEventStream
    .transform(MigrationAssessmentTransformer(MigrationAssessmentSourceTypes.EventHubRawEventStream, spark).transform)
    .transform(MigrationAssessmentTransformer(MigrationAssessmentSourceTypes.SkuRecommendationMI, spark).transform)
    .transform(MigrationAssessmentTransformer(MigrationAssessmentSourceTypes.PricingComputation, spark, PlatformType.AzureSqlManagedInstance, computeDF=computeMIDF, storageDF=storagePaasDF).transform)

  val skuVmDF = skuVmEventStream
    .transform(MigrationAssessmentTransformer(MigrationAssessmentSourceTypes.EventHubRawEventStream, spark).transform)
    .transform(MigrationAssessmentTransformer(MigrationAssessmentSourceTypes.SkuRecommendationVM, spark).transform)
    .transform(MigrationAssessmentTransformer(MigrationAssessmentSourceTypes.PricingComputation, spark, PlatformType.AzureSqlVirtualMachine, computeDF=computeVMDF, storageDF=storageVMDF).transform)

  val outputDF = suitDF.transform(
                  MigrationAssessmentTransformer(
                    resourceType = MigrationAssessmentSourceTypes.FullAssessment, 
                    spark = spark, 
                    skuDbDF = skuDbDF, 
                    skuMiDF = skuMiDF, 
                    skuVmDF = skuVmDF
                  ).transform
                ).transform(
                  MigrationAssessmentTransformer(MigrationAssessmentSourceTypes.InstanceUpdate, spark).transform
                )

  outputDF.printSchema()

  val serializedDF = outputDF
  .select(to_json(struct("*")).cast("string").alias("body"))
  .selectExpr("CAST(body AS BINARY) AS body")

  serializedDF.printSchema()
  println("Starting write to event hub....")
  val outputFeed = WriteEventHubFeedFormat(sinkEventHubConnection, eventHubCheckPointLocation, authCallback)
  EventHubStreamFeed(outputFeed, spark).write(serializedDF)
  
}
