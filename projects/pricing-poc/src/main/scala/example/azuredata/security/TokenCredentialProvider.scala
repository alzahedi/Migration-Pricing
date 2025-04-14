package example.azuredata.security

import com.azure.identity.{
  DefaultAzureCredentialBuilder,
  AzureCliCredentialBuilder,
  ManagedIdentityCredentialBuilder
}
import com.azure.core.credential.TokenCredential

/** Factory to provide appropriate TokenCredential based on environment or
  * preference.
  */
object TokenCredentialProvider {

  /** Auto-detects best available credential. */
  def getDefaultCredential: TokenCredential = {
    new DefaultAzureCredentialBuilder().build()
  }

  /** Use Azure CLI (typically for local dev). */
  def getAzureCliCredential: TokenCredential = {
    new AzureCliCredentialBuilder().build()
  }

  /** Use Managed Identity (for Azure-hosted services). */
  def getManagedIdentityCredential(
      clientId: Option[String] = None
  ): TokenCredential = {
    clientId match {
      case Some(id) =>
        new ManagedIdentityCredentialBuilder().clientId(id).build()
      case None => new ManagedIdentityCredentialBuilder().build()
    }
  }

}
