package example.reader

import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.{DataFrame, SparkSession}
import example.service.BlobService
import example.constants.PlatformType

object BlobReader {

  def readJsonFilesFromBlob(
      spark: SparkSession,
      accountName: String,
      containerName: String
  ): Map[PlatformType, Map[String, DataFrame]] = {

    val fileMappings: Map[PlatformType, Map[String, String]] = Map(
      PlatformType.AzureSqlDatabase -> Map(
        "Compute" -> "SQL_DB_Compute.json",
        "License" -> "SQL_DB_License.json",
        "Storage" -> "SQL_DB_Storage.json"
      ),
      PlatformType.AzureSqlManagedInstance -> Map(
        "Compute" -> "SQL_MI_Compute.json",
        "License" -> "SQL_MI_License.json",
        "Storage" -> "SQL_MI_Storage.json"
      ),
      // PlatformType.AzureSqlVirtualMachine -> Map(
      //   "Compute" -> "SQL_VM_Compute.json",
      //   "Storage" -> "SQL_VM_Storage.json"
      // )
    )

    val jsonFiles = BlobService.listJsonFiles(accountName, containerName)

    if (jsonFiles.isEmpty) {
      throw new RuntimeException("No JSON files found in the container!")
    }

    // Generate a **single SAS token for the container**
    val containerSasToken = BlobService.generateContainerSas(accountName, containerName)
    val baseBlobUrl = s"wasbs://$containerName@$accountName.blob.core.windows.net"

    // Configure Spark to use the **same SAS token for all files**
    spark.conf.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    spark.conf.set(
      s"fs.azure.sas.$containerName.$accountName.blob.core.windows.net",
      containerSasToken
    )

    // Read files into structured Map
    val dataFrames = fileMappings.map { case (platform, categories) =>
      val categoryDataFrames = categories.flatMap { case (category, fileName) =>
        if (jsonFiles.contains(fileName)) {
          val blobUrl = s"$baseBlobUrl/$fileName"
          Try(spark.read.option("multiline", "true").json(blobUrl)) match {
            case Success(df) =>
              println(s"\n ✅ Successfully loaded $fileName into DataFrame for $category ($platform)")
              Some(category -> df)
            case Failure(exception) =>
              println(s"❌ Failed to read $fileName: ${exception.getMessage}")
              exception.printStackTrace()
              None
          }
        } else {
          println(s"⚠️ File $fileName not found in the container!")
          None
        }
      }
      platform -> categoryDataFrames
    }
    dataFrames
  }
}
