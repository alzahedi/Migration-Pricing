package example.eventhub

import org.apache.spark.sql.{DataFrame, SparkSession}

class EventHubStreamFeed(feed: EventHubFeedFormat, spark: SparkSession) {
  def read(): DataFrame = {
    spark.readStream
      .format("eventhubs")
      .options(feed.readerEventHubsConf.toMap)
      .load()
  }

  def write(df: DataFrame): Unit = {
    df
      .writeStream
      .outputMode("append")
      .format("eventhubs")
      .options(feed.writerEventHubsConf.toMap)
      .option("checkpointLocation", feed.checkpointLocation) 
      .start()
      .awaitTermination()
  }
}

object EventHubStreamFeed {
  def apply(feed: EventHubFeedFormat, spark: SparkSession): EventHubStreamFeed =
    new EventHubStreamFeed(feed, spark)
}
