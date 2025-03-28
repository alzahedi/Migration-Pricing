package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.eventhubs._
import java.net.URI

import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.core.credential.TokenRequestContext
import org.apache.spark.eventhubs.utils.AadAuthenticationCallback
import java.util.concurrent.CompletableFuture
import java.nio.file.Paths
import com.azure.identity.AzureCliCredentialBuilder

object StreamDriver extends App{

val log4jConfigPath = Paths.get(System.getProperty("user.dir"), "log4j2.properties").toString
System.setProperty("log4j.configurationFile", s"file://$log4jConfigPath")
   
val spark = SparkSession.builder()
  .appName("EventHubReader")
  .master("local[*]") 
  .getOrCreate()

val eventHubNamespace = "pricing-streaming"
val eventHubName = "streaming-input"

val credential = new AzureCliCredentialBuilder().build()

val tokenContext = new TokenRequestContext()
  .addScopes("https://eventhubs.azure.net/.default")

val aadAuthCallback = new AadAuthenticationCallback {
  
  // Provide authority (default Microsoft Entra ID endpoint)
  override def authority: String = "https://login.microsoftonline.com/"
  
  // Acquire token asynchronously
  override def acquireToken(audience: String, authority: String, state: Any): CompletableFuture[String] = {
    CompletableFuture.supplyAsync(() => credential.getToken(tokenContext).block().getToken)
  }
}

// Configure Event Hubs with Microsoft Entra ID authentication
val eventHubsConf = EventHubsConf(s"Endpoint=sb://$eventHubNamespace.servicebus.windows.net/;EntityPath=$eventHubName")
  .setAadAuthCallback(aadAuthCallback)
  .setStartingPosition(EventPosition.fromStartOfStream)

val eventStream = spark.readStream
  .format("eventhubs")
  .options(eventHubsConf.toMap)
  .load()

val messages = eventStream.selectExpr("CAST(body AS STRING) AS message")

val query = messages.writeStream
  .outputMode("append")
  .format("console")
  .start()

query.awaitTermination()

}
