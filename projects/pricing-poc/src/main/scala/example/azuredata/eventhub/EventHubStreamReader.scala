package example.azuredata.eventhub

import org.apache.spark.sql.{DataFrame, SparkSession}

class EventHubStreamReader(feed: EventHubFeedFormat)(implicit spark: SparkSession) {
  def read(): DataFrame = {
    spark.readStream
      .format("eventhubs")
      .options(feed.readerEventHubsConf.toMap)
      .load()
  }
}

object EventHubStreamReader {
  def apply(feed: EventHubFeedFormat)(implicit spark: SparkSession): EventHubStreamReader =
    new EventHubStreamReader(feed)
}
