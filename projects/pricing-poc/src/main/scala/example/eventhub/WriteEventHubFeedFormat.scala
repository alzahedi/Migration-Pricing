package example.eventhub

import org.apache.spark.eventhubs.EventHubsConf

class WriteEventHubFeedFormat(
    userWriterEventHubsConf: EventHubsConf,
    userCheckpointLocation: String
) extends EventHubFeedFormat {

  /** @inheritdoc
    */
  override def writerEventHubsConf: EventHubsConf = userWriterEventHubsConf

  /** @inheritdoc
    */
  override def checkpointLocation: String = userCheckpointLocation

}

object WriteEventHubFeedFormat{
    def apply(
      eventHubConnection: EventHubConnection,
      checkpointLocation: String,
      entraCallback: EventHubEntraAuthCallback = null,
    ): WriteEventHubFeedFormat = {
        var eventHubConf = EventHubsConf(eventHubConnection.connectionString)

        if(entraCallback != null){
            eventHubConf.setAadAuthCallback(entraCallback)
        }

        new WriteEventHubFeedFormat(eventHubConf, checkpointLocation)
    }
}