package example.eventhub

import org.apache.spark.sql.{DataFrame, SparkSession}

class EventHubStreamFeed(feed: EventHubFeedFormat, spark: SparkSession) {
  def read(): DataFrame = {
    spark.readStream
      .format("eventhubs")
      .options(feed.readerEventHubsConf.toMap)
      .load()
  }
}

object EventHubStreamFeed {
  def apply(feed: EventHubFeedFormat, spark: SparkSession): EventHubStreamFeed =
    new EventHubStreamFeed(feed, spark)
}
