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

  // Azure CLI Authentication
  val credential = new AzureCliCredentialBuilder().build()
  val tokenContext = new TokenRequestContext().addScopes("https://eventhubs.azure.net/.default")

  val aadAuthCallback = new AadAuthenticationCallback {
    override def authority: String = "https://login.microsoftonline.com/"
    override def acquireToken(audience: String, authority: String, state: Any): CompletableFuture[String] = {
      CompletableFuture.supplyAsync(() => credential.getToken(tokenContext).block().getToken)
    }
  }

  // Configure Event Hubs
  val eventHubsConf = EventHubsConf(s"Endpoint=sb://$eventHubNamespace.servicebus.windows.net/;EntityPath=$eventHubName")
    .setAadAuthCallback(aadAuthCallback)
    .setStartingPosition(EventPosition.fromStartOfStream)

  val eventStream = spark.readStream
    .format("eventhubs")
    .options(eventHubsConf.toMap)
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

  val parsedStream = eventStream
    .selectExpr("CAST(body AS STRING) AS message", "enqueuedTime")
    .select(from_json($"message", eventSchema).as("data"), $"enqueuedTime")
    .select("data.*", "enqueuedTime")
    .withColumn("timestamp", current_timestamp())

  // Accumulate messages by uploadIdentifier
  // val aggregatedStream = parsedStream
  //   .withWatermark("timestamp", "5 minutes")
  //   .groupBy($"uploadIdentifier")
  //   .agg(collect_list($"body").as("messages"))

  // val query = aggregatedStream.writeStream
  //   .outputMode("update")
  //   .format("console")
  //   .start()

  val suitDF = processData(parsedStream, MigrationAssessmentSourceTypes.Suitability)
                .transform(TransformationsV1.transformSuitability)
  
  val skuDbDF = processData(parsedStream, MigrationAssessmentSourceTypes.SkuRecommendationDB)
                .transform(TransformationsV1.processSkuData(PlatformType.AzureSqlDatabase, 
                 schemaStructNames(MigrationAssessmentSourceTypes.SkuRecommendationDB)))

  val skuMiDF = processData(parsedStream, MigrationAssessmentSourceTypes.SkuRecommendationMI)
                .transform(TransformationsV1.processSkuData(PlatformType.AzureSqlManagedInstance,
                 schemaStructNames(MigrationAssessmentSourceTypes.SkuRecommendationMI)))

  val skuVmDF = processData(parsedStream, MigrationAssessmentSourceTypes.SkuRecommendationVM)
                .transform(TransformationsV1.processSkuData(PlatformType.AzureSqlVirtualMachine,
                schemaStructNames(MigrationAssessmentSourceTypes.SkuRecommendationVM)))

  suitDF.printSchema()
  skuDbDF.printSchema()
  skuMiDF.printSchema()
  skuVmDF.printSchema()

  spark.conf.set("spark.sql.streaming.join.debug", "true")
  val joinedDF = suitDF.as("es")
                  .join(skuDbDF.as("esrasd"), expr("""(es.uploadIdentifier == esrasd.uploadIdentifier) AND (es.enqueuedTime BETWEEN (esrasd.enqueuedTime - INTERVAL 5 MINUTES) AND (esrasd.enqueuedTime + INTERVAL 5 MINUTES))"""), joinType = "inner")
                  // .join(skuMiDF.as("esrasm"), expr(s"""es.uploadIdentifier == esrasm.uploadIdentifier AND es.enqueuedTime BETWEEN esrasm.enqueuedTime - ${MigrationAssessmentConstants.DefaultAcrossStreamsIntervalMaxLag} AND esrasm.enqueuedTime + ${MigrationAssessmentConstants.DefaultAcrossStreamsIntervalMaxLag}"""), joinType = "inner")
                  // .join(skuVmDF.as("esrasv"), expr(s"""es.uploadIdentifier == esrasv.uploadIdentifier  AND es.enqueuedTime BETWEEN esrasv.enqueuedTime - ${MigrationAssessmentConstants.DefaultAcrossStreamsIntervalMaxLag} AND esrasv.enqueuedTime + ${MigrationAssessmentConstants.DefaultAcrossStreamsIntervalMaxLag}"""), joinType = "inner")
                  .drop("type")
                  .drop("timestamp")
                  .drop("uploadIdentifier")

  joinedDF.printSchema()
  joinedDF.explain()
 

  // Process the result, e.g., showing it or saving to a file
  val query = joinedDF.writeStream
    .outputMode("append")
    .option("checkpointLocation", "/workspaces/pricing-poc/projects/pricing-poc/src/main/resources/output/checkpoint")
    .trigger(Trigger.ProcessingTime("60 seconds")).queryName("myTable")
    .format("memory")
    .start()

  // println("Checking data.....")
  // spark.sql("SELECT * FROM myTable").show(10000, true)
  while(true) {  // Runs exactly 5 times
    println("Checking data.....")
    Thread.sleep(1000)
    //spark.sql("SELECT uploadIdentifier, enqueuedTime FROM myTable").show(10000, true)
    spark.sql("SELECT * FROM myTable").show(10000, true)
  }
  //query.awaitTermination()

  def processData(inDF: DataFrame, maType: MigrationAssessmentSourceTypes.Value): DataFrame = {
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
}
