import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.eventhubs._
import com.azure.identity.AzureCliCredentialBuilder
import com.azure.core.credential.TokenRequestContext
import org.apache.spark.eventhubs.utils.AadAuthenticationCallback
import java.util.concurrent.CompletableFuture
import java.nio.file.Paths

object StreamDriver extends App {

  // Load Log4j Configuration
  val log4jConfigPath = Paths.get(System.getProperty("user.dir"), "log4j2.properties").toString
  System.setProperty("log4j.configurationFile", s"file://$log4jConfigPath")

  // Initialize Spark session
  val spark = SparkSession.builder()
    .appName("EventHubReader")
    .master("local[*]") 
    .getOrCreate()

  import spark.implicits._

  // Event Hub details
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

  // Read the Event Hub stream
  val eventStream = spark.readStream
    .format("eventhubs")
    .options(eventHubsConf.toMap)
    .load()

  // Define JSON schema
  val eventSchema = StructType(Seq(
    StructField("uploadIdentifier", StringType, nullable = false),
    StructField("type", StringType, nullable = false),
    StructField("body", StringType, nullable = false)
  ))

  // Parse JSON messages
  val parsedStream = eventStream
    .selectExpr("CAST(body AS STRING) AS message")
    .select(from_json($"message", eventSchema).as("data"))
    .select("data.*")
    .withColumn("timestamp", current_timestamp())  // Add event time column

  // Accumulate messages by uploadIdentifier
  val aggregatedStream = parsedStream
    .withWatermark("timestamp", "5 minutes")  // Allow late messages up to 5 minutes
    .groupBy($"uploadIdentifier")
    .agg(collect_list($"body").as("messages"))  // Collect all messages for the same uploadIdentifier

  // Write accumulated messages to console
  val query = aggregatedStream.writeStream
    .outputMode("update")  // Only updated groups are outputted
    .format("console")
    .start()

  query.awaitTermination()
}
