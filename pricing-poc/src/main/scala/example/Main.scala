package example
import example.azure.BlobReader
import example.utils.SparkUtils

object Main extends App {

  val accountName = "guptasnigdhatest"
  val containerName = "pricing"

  val spark = SparkUtils.createSparkSession()

  try {
    BlobReader.readJsonFilesFromBlob(spark, accountName, containerName)
  } finally {
    spark.stop()
  }

}
