package example.reader

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import example.service.BlobService

class BlobReader(fileName: String, spark: SparkSession) extends DataReader {

  override def read(): DataFrame = {
    val accountName = "guptasnigdhatest"
    val containerName = "pricing"

    val containerSasToken = BlobService.generateContainerSas(accountName, containerName)
    val baseBlobUrl = s"wasbs://$containerName@$accountName.blob.core.windows.net"

    // Configure Spark to use the **same SAS token for all files**
    spark.conf.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    spark.conf.set(
      s"fs.azure.sas.$containerName.$accountName.blob.core.windows.net", containerSasToken
    )

    val blobUrl = s"$baseBlobUrl/$fileName"
    JsonReader(blobUrl, spark).read()
    
  }
}

object BlobReader{
  def apply(fileName: String, spark: SparkSession): BlobReader = {
    new BlobReader(fileName, spark)
  }
}