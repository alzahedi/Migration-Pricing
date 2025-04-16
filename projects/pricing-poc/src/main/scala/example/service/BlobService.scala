package example.service

import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.storage.blob.{BlobContainerClient, BlobServiceClientBuilder}
import com.azure.storage.blob.models.UserDelegationKey
import com.azure.storage.blob.sas.{
  BlobContainerSasPermission,
  BlobServiceSasSignatureValues
}
import com.azure.storage.common.sas.SasProtocol

import java.time.OffsetDateTime
import scala.collection.JavaConverters._
import example.security.TokenCredentialProvider

object BlobService {

  private def getBlobServiceClient(accountName: String) = {
    val credential = TokenCredentialProvider.getAzureCliCredential
    new BlobServiceClientBuilder()
      .credential(credential)
      .endpoint(s"https://$accountName.blob.core.windows.net")
      .buildClient()
  }

  /** Generate a single SAS token for the entire container */
  def generateContainerSas(
      accountName: String,
      containerName: String
  ): String = {
    val blobServiceClient = getBlobServiceClient(accountName)

    val expiryTime = OffsetDateTime.now().plusHours(1)
    val delegationKey: UserDelegationKey =
      blobServiceClient.getUserDelegationKey(OffsetDateTime.now(), expiryTime)

    val permissions = new BlobContainerSasPermission().setReadPermission(true)
    val sasValues = new BlobServiceSasSignatureValues(expiryTime, permissions)
      .setProtocol(SasProtocol.HTTPS_ONLY)

    val containerClient =
      blobServiceClient.getBlobContainerClient(containerName)

    containerClient.generateUserDelegationSas(sasValues, delegationKey)
  }
}
