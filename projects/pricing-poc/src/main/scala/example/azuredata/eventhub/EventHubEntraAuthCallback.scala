package example.azuredata.eventhub

import example.azuredata.security.BaseAadAuthCallback
import example.azuredata.security.TokenCredentialProvider
import com.azure.core.credential.TokenCredential

// Primary constructor (for manual use)
class EventHubEntraAuthCallback(credential: TokenCredential)
  extends BaseAadAuthCallback(credential, "https://eventhubs.azure.net/.default") {

  // Default constructor required by Spark (uses Azure CLI credential)
  def this() = this(EventHubEntraAuthCallback.defaultCredential)
}

object EventHubEntraAuthCallback {
  def apply(credential: TokenCredential): EventHubEntraAuthCallback = {
    new EventHubEntraAuthCallback(credential)
  }

  lazy val defaultCredential: TokenCredential = TokenCredentialProvider.getAzureCliCredential
}
