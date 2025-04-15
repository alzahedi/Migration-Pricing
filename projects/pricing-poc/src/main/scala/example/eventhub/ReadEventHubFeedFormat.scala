package example.eventhub

import org.apache.spark.eventhubs.{EventHubsConf,EventPosition}
import org.apache.spark.eventhubs.utils.AadAuthenticationCallback

class ReadEventHubFeedFormat(
    userReaderEventHubsConf: EventHubsConf,
    specifiedConsumerGroup: String
) extends EventHubFeedFormat {

  /** @inheritdoc
    */
  override def readerEventHubsConf: EventHubsConf = userReaderEventHubsConf

  /** @inheritdoc
    */
  override def consumerGroup: String = specifiedConsumerGroup
}

object ReadEventHubFeedFormat{
    def apply(
      readerStartingPosition: EventPosition = EventPosition.fromStartOfStream,
      eventHubConnection: EventHubConnection,
      specifiedConsumerGroup: String = "$Default",
      entraCallback: EventHubEntraAuthCallback = null,
    ): ReadEventHubFeedFormat = {
        var eventHubConf = EventHubsConf(eventHubConnection.connectionString)
            .setStartingPosition(readerStartingPosition)
            .setConsumerGroup(specifiedConsumerGroup)

        if(entraCallback != null){
            eventHubConf.setAadAuthCallback(entraCallback)
        }

        new ReadEventHubFeedFormat(eventHubConf, specifiedConsumerGroup)
    }
}
