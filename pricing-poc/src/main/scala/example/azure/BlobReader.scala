package example.azure
import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.{DataFrame, SparkSession}

object BlobReader {

  def readJsonFilesFromBlob(
      spark: SparkSession,
      accountName: String,
      containerName: String
  ): Unit = {

    val jsonFiles = BlobService.listJsonFiles(accountName, containerName)

    if (jsonFiles.isEmpty) {
      throw new RuntimeException("No JSON files found in the container!")
    }

    // Generate a **single SAS token for the container**
    val containerSasToken =
      BlobService.generateContainerSas(accountName, containerName)
    val baseBlobUrl =
      s"wasbs://$containerName@$accountName.blob.core.windows.net"

    // Configure Spark to use the **same SAS token for all files**
    spark.conf.set(
      "fs.azure",
      "org.apache.hadoop.fs.azure.NativeAzureFileSystem"
    )
    spark.conf.set(
      s"fs.azure.sas.$containerName.$accountName.blob.core.windows.net",
      containerSasToken
    )

    jsonFiles.foreach { blobName =>
      val blobUrl = s"$baseBlobUrl/$blobName"

      Try(spark.read.option("multiline", "true").json(blobUrl)) match {
        case Success(df) =>
          println(s"\n🔹 Processing file: $blobName")
          df.show() // Process each DataFrame separately (Modify as needed)

        case Failure(exception) =>
          println(s"❌ Failed to read $blobName: ${exception.getMessage}")
          exception.printStackTrace()
      }
    }
  }

}
