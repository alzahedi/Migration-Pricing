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

object StreamDriver extends App {

  val reportsDirPath = Paths.get(System.getProperty("user.dir"), "src", "main", "resources", "reports").toString
  val log4jConfigPath = Paths.get(System.getProperty("user.dir"), "log4j2.properties").toString
  System.setProperty("log4j.configurationFile", s"file://$log4jConfigPath")

  implicit val spark = SparkSession.builder()
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

  // Create auth callback
  val authCallback = EventHubEntraAuthCallback(TokenCredentialProvider.getAzureCliCredential)
  val eventHubConnection = EventHubConnection(eventHubNamespace, eventHubName)
  
  val suitabilityEventHubFeed = ReadEventHubFeedFormat(
                                  eventHubConnection = eventHubConnection, 
                                  specifiedConsumerGroup = suitabilityConsumerGroup, 
                                  entraCallback = authCallback
                                )

  val skuDbEventHubFeed = ReadEventHubFeedFormat(
                            eventHubConnection = eventHubConnection,
                            specifiedConsumerGroup = skuDbConsumerGroup,
                            entraCallback = authCallback
                          )

  val skuMiEventHubFeed = ReadEventHubFeedFormat(
                            eventHubConnection = eventHubConnection,
                            specifiedConsumerGroup = skuMiConsumerGroup,
                            entraCallback = authCallback
                          )

  val skuVmEventHubFeed = ReadEventHubFeedFormat(
                            eventHubConnection = eventHubConnection,
                            specifiedConsumerGroup = skuVmConsumerGroup,
                            entraCallback = authCallback
                          )



  val outputEventHubConf = EventHubsConf(s"Endpoint=sb://$eventHubNamespace.servicebus.windows.net/;EntityPath=$outputEventHubName")
    .setAadAuthCallback(authCallback)

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
    .transform(MigrationAssessmentTransformer(MigrationAssessmentSourceTypes.PricingComputation, spark, PlatformType.AzureSqlDatabase).transform)
  
  val skuMiDF = skuMiEventStream
    .transform(MigrationAssessmentTransformer(MigrationAssessmentSourceTypes.EventHubRawEventStream, spark).transform)
    .transform(MigrationAssessmentTransformer(MigrationAssessmentSourceTypes.SkuRecommendationMI, spark).transform)
    .transform(MigrationAssessmentTransformer(MigrationAssessmentSourceTypes.PricingComputation, spark, PlatformType.AzureSqlManagedInstance).transform)

  val skuVmDF = skuVmEventStream
    .transform(MigrationAssessmentTransformer(MigrationAssessmentSourceTypes.EventHubRawEventStream, spark).transform)
    .transform(MigrationAssessmentTransformer(MigrationAssessmentSourceTypes.SkuRecommendationVM, spark).transform)
    .transform(MigrationAssessmentTransformer(MigrationAssessmentSourceTypes.PricingComputation, spark, PlatformType.AzureSqlVirtualMachine).transform)

  spark.conf.set("spark.sql.streaming.join.debug", "true")
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

  // val outputDF = processInstanceUpdateEventStream(joinedDF)
  outputDF.printSchema()


  // Process the result, e.g., showing it or saving to a file
  // val query = joinedDF.writeStream
  //   .outputMode("append")
  //   .option("checkpointLocation", "/workspaces/Migration-Pricing/projects/pricing-poc/src/main/resources/output/checkpoint")
  //   .trigger(Trigger.ProcessingTime("60 seconds")).queryName("myTable")
  //   .format("memory")
  //   .start()

  // while(true) {
  //   println("Checking data.....")
  //   Thread.sleep(1000)
  //   //spark.sql("SELECT computeCost_1Yr, computeCost_3Yr, monthlyCostOptions FROM myTable").show(10000, true)
  //   spark.sql("SELECT * FROM myTable").show(10000, true)
  // }

  val serializedDF = outputDF
  .select(to_json(struct("*")).cast("string").alias("body"))
  .selectExpr("CAST(body AS BINARY) AS body")

  // val serializedDF = outputDF
  //   .select(to_json(struct("*")).alias("body"))
  //   .selectExpr("CAST(body AS BINARY) AS body")

  serializedDF.printSchema()
  println("Starting write to event hub....")
  val query = serializedDF
    .writeStream
    .outputMode("append")
    .format("eventhubs")
    .options(outputEventHubConf.toMap)
    .option("checkpointLocation", "/workspaces/Migration-Pricing/projects/pricing-poc/src/main/resources/output/eventhub-checkpoint") // Must be a reliable location like DBFS or HDFS
    .start()
    .awaitTermination()

  
}
