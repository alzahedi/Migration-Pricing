package example.security

import com.azure.core.credential.{AccessToken, TokenCredential, TokenRequestContext}
import org.apache.spark.eventhubs.utils.AadAuthenticationCallback

import java.util.concurrent.{CompletableFuture, Executors}
import java.util.function.Supplier

abstract class BaseAadAuthCallback(
    credential: TokenCredential,
    scope: String
) extends AadAuthenticationCallback {

  private val tokenContext = new TokenRequestContext().addScopes("https://eventhubs.azure.net/.default")

  override def authority: String = "https://login.microsoftonline.com/"

  override def acquireToken(audience: String, authority: String, state: Any): CompletableFuture[String] = {
    CompletableFuture.supplyAsync(() => credential.getToken(tokenContext).block().getToken)
  }
}

