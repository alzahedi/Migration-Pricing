package example

import example.reader.JsonReader
import example.utils.SparkUtils
import example.writer.JsonWriter
import example.transformations.Transformations
import java.nio.file.Paths
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main extends App {

  val reportsDirPath = Paths.get(System.getProperty("user.dir"), "src", "main", "resources", "reports").toString
  val log4jConfigPath = Paths.get(System.getProperty("user.dir"), "log4j2.properties").toString
  System.setProperty("log4j.configurationFile", s"file://$log4jConfigPath")

  val spark = SparkUtils.createSparkSession()
  try {
    import spark.implicits._

    // Load JSON files
    val jsonPaths = Map(
      "sku-db" -> Paths.get(reportsDirPath, "sku", "sku-db.json").toString,
      "sku-mi" -> Paths.get(reportsDirPath, "sku", "sku-mi.json").toString,
      "sku-vm" -> Paths.get(reportsDirPath, "sku", "sku-vm.json").toString,
      "suitability" -> Paths.get(reportsDirPath, "suitability", "suit.json").toString,
      "vm-schema" -> Paths.get(reportsDirPath, "samples", "sku-vm-full-fidelity.json").toString
    )

    val dbDf = JsonReader.readJson(spark, jsonPaths("sku-db"))
    val miDf = JsonReader.readJson(spark, jsonPaths("sku-mi"))
    val vmDf = JsonReader.readJsonWithSchemaInferred(spark, jsonPaths("sku-vm"), jsonPaths("vm-schema"))
    val suitabilityDf = JsonReader.readJson(spark, jsonPaths("suitability")).transform(Transformations.transformSuitability)

    // Process transformations
    val dbTransformedDf = Transformations.processSkuData(dbDf, "azureSqlDatabase", suitabilityDf)
    val miTransformedDf = Transformations.processSkuData(miDf, "azureSqlManagedInstance", suitabilityDf)
    val vmTransformedDf = Transformations.processSkuData(vmDf, "azureSqlVirtualMachine", suitabilityDf)

    // Print results
    Seq(dbTransformedDf, miTransformedDf, vmTransformedDf).foreach { df =>
      df.printSchema()
      df.show(false)
    }

    // Aggregate
    val jsonResultDf = Transformations.aggregateSkuRecommendations(dbTransformedDf, miTransformedDf, vmTransformedDf)
    jsonResultDf.printSchema()
    jsonResultDf.show(false)

    val outputPath = Paths
      .get(
        System.getProperty("user.dir"),
        "src",
        "main",
        "resources",
        "output",
        "output.json"
      )
      .toString

    JsonWriter.writeToJsonFile(jsonResultDf, outputPath)
  } finally {
    spark.stop()
  }
}
