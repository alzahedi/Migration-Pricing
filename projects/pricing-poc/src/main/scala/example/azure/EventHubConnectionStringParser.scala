package example.azure

// @formatter:off
/** Represents a parsed Event Hub connection.
  *
  * E.g. say - for:
  *
  * >>> Endpoint=sb://arndatareplicate.servicebus.windows.net/;SharedAccessKeyName=listen-events-policy;SharedAccessKey=n25...;EntityPath=armlinkednotifications-australiaeast
  *
  * @param namespace
  *   The namespace of the Event Hub:
  *
  *   >>> arndatareplicate
  *
  * @param namespaceFqdn
  *   The fully qualified domain name of the namespace:
  *
  *   >>>  sb://arndatareplicate.servicebus.windows.net/
  *
  * @param namespaceFqdnDomainOnly
  *   The domain name of the namespace without protocol:
  *
  *   >>> arndatareplicate.servicebus.windows.net
  *
  * @param eventHub
  *   The name of the Event Hub:
  *
  *   >>> armlinkednotifications-australiaeast
  *
  * @param sharedAccessKeyName
  *   The shared access key name:
  *
  *   >>> listen-events-policy
  *
  * @param sharedAccessKey
  *   The shared access key:
  *
  *   >>> n25...
  *
  * @param connectionString
  *   The original connection string.
  *
  *   >>> Endpoint=sb://arndatareplicate.servicebus.windows.net/;SharedAccessKeyName=listen-events-policy;SharedAccessKey=n25...;EntityPath=armlinkednotifications-australiaeast
  */
case class EventHubConnection(
    namespace: String,
    namespaceFqdn: String,
    namespaceFqdnDomainOnly: String,
    eventHub: String,
    sharedAccessKeyName: String,
    sharedAccessKey: String,
    connectionString: String
)

object EventHubConnectionStringParser {

  /** Parses an Event Hub connection string and returns a parsed
    * EventHubConnection object.
    *
    * @param connectionString
    *   The Event Hub connection string to parse.
    * @return
    *   The parsed EventHubConnection object.
    */
  def parse(connectionString: String): EventHubConnection = {
    val parts = connectionString.split(";").map(_.trim)
    val props = parts
      .map(_.split("=", 2))
      .collect { case Array(key, value) =>
        (key.toLowerCase, value)
      }
      .toMap

    val namespace = props.getOrElse("endpoint", "").split('.')(0).substring(5)
    val namespaceFqdn = props.getOrElse("endpoint", "")
    val namespaceFqdnDomainOnly = namespaceFqdn.stripPrefix("sb://").stripSuffix("/")
    val eventHub = props.getOrElse("entitypath", "")
    val sharedAccessKeyName = props.getOrElse("sharedaccesskeyname", "")
    val sharedAccessKey = props.getOrElse("sharedaccesskey", "")
    if (namespace.isEmpty || namespaceFqdn.isEmpty || namespaceFqdnDomainOnly.isEmpty || eventHub.isEmpty) {
      throw new IllegalArgumentException(
        s"Invalid Event Hub connection string: ${connectionString}"
      )
    }

    EventHubConnection(
      namespace = namespace,
      namespaceFqdn = namespaceFqdn,
      namespaceFqdnDomainOnly = namespaceFqdnDomainOnly,
      eventHub = eventHub,
      sharedAccessKeyName = sharedAccessKeyName,
      sharedAccessKey = sharedAccessKey,
      connectionString = connectionString
    )
  }
}
// @formatter:on

