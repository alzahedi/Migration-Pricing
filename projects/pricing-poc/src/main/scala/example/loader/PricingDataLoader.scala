package example.loader

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.nio.file.Paths
import example.constants.PlatformType
import example.reader.JsonReader
import example.reader.BlobReader


class PricingDataLoader(platformType: PlatformType, category: String, spark: SparkSession) extends DataLoader {

  override def load(): DataFrame = {
    val fileMappings: Map[PlatformType, Map[String, String]] = Map(
      PlatformType.AzureSqlDatabase -> Map("Compute" -> "SQL_DB_Compute.json", "Storage" -> "SQL_DB_Storage.json", "License" -> "SQL_DB_License.json"),
      PlatformType.AzureSqlManagedInstance -> Map("Compute" -> "SQL_MI_Compute.json", "Storage" -> "SQL_MI_Storage.json", "License" -> "SQL_MI_License.json"),
      PlatformType.AzureSqlVirtualMachine -> Map("Compute" -> "SQL_VM_Compute.json", "Storage" -> "SQL_VM_Storage.json", "License" -> "Azure_Hybrid_Benefit.json")
    )

    val fileName = fileMappings
      .get(platformType)
      .flatMap(_.get(category))
      .getOrElse(throw new IllegalArgumentException(s"No file found for platform: $platformType and category: $category"))

    BlobReader(fileName, spark).read().selectExpr("explode(Content) as Content").select("Content.*")
  }
}

object PricingDataLoader {
  def apply(platformType: PlatformType, category: String, spark: SparkSession): PricingDataLoader = {
    new PricingDataLoader(platformType, category, spark)
  }
}
