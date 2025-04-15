package example.eventhub

import org.apache.spark.eventhubs.EventHubsConf
import org.apache.spark.sql.streaming.Trigger

/** Represents an Event Hub feed.
  */
trait EventHubFeedFormat {

  /** Gets the name of the Event Hubs Namespace associated with the feed.
    */
  def namespace: String = ""

  /** Gets the name of the Event Hub associated with the feed.
    */
  def eventHub: String = ""

  /** Gets the name of the Consumer Group associated with the feed to read from.
    */
  def consumerGroup: String = "$Default"

  /** Gets the schema of the feed, represented as an array of column names and
    * their corresponding data types.
    */
  def schema: Array[(String, String)] = Array()

  /** Gets the reader Event Hubs Config.
    */
  def readerEventHubsConf: EventHubsConf = ???

  /** Gets the writer Event Hubs Config.
    */
  def writerEventHubsConf: EventHubsConf = ???

  /** Checkpoint location
    */
  def checkpointLocation: String = ???
}
