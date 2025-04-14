package example.loader

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.nio.file.Paths
import example.constants.PlatformType
import example.reader.JsonReader

object PricingDataLoader {
  def load(platformType: PlatformType)(implicit spark: SparkSession): Map[String, DataFrame] = {
    val fileMappings: Map[PlatformType, Map[String, String]] = Map(
      PlatformType.AzureSqlDatabase -> Map("Compute" -> "SQL_DB_Compute.json", "Storage" -> "SQL_DB_Storage.json"),
      PlatformType.AzureSqlManagedInstance -> Map("Compute" -> "SQL_MI_Compute.json", "Storage" -> "SQL_MI_Storage.json"),
      PlatformType.AzureSqlVirtualMachine -> Map("Compute" -> "SQL_VM_Compute.json", "Storage" -> "SQL_VM_Storage.json")
    )

    val basePath = Paths.get(System.getProperty("user.dir"), "src", "main", "resources", "pricing").toString
    val fileMapping = fileMappings.getOrElse(platformType, Map.empty)

    fileMapping.map { case (category, fileName) =>
      val filePath = s"$basePath/$fileName"
      val rawDF = JsonReader.readJson(spark, filePath)
      val contentDF = rawDF.selectExpr("explode(Content) as Content").select("Content.*")
      category -> contentDF
    }
  }
}
