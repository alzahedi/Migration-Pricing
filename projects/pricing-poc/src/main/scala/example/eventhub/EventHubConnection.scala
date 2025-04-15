package example.eventhub

case class EventHubConnection(
  namespace: String,
  eventHubName: String,
  connectionString: String
)

object EventHubConnection {
  def apply(namespace: String, eventHubName: String): EventHubConnection = {
    val connectionString =
      s"Endpoint=sb://$namespace.servicebus.windows.net/;EntityPath=$eventHubName"

    new EventHubConnection(namespace, eventHubName, connectionString)
  }
}
