package example

import example.reader.JsonReader
import example.utils.SparkUtils
import example.writer.JsonWriter
import java.nio.file.Paths
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import example.constants.PricingType
import org.apache.spark.sql.Column
import example.constants.PlatformType

object MainIaas extends App {
  val reportsDirPath = Paths
    .get(System.getProperty("user.dir"), "src", "main", "resources", "reports")
    .toString
  val pricingDirPath = Paths
    .get(System.getProperty("user.dir"), "src", "main", "resources", "pricing")
    .toString
  val sampleDirPath = Paths
    .get(System.getProperty("user.dir"), "src", "main", "resources", "samples")
    .toString
  val log4jConfigPath =
    Paths.get(System.getProperty("user.dir"), "log4j2.properties").toString
  System.setProperty("log4j.configurationFile", s"file://$log4jConfigPath")

  implicit val spark = SparkSession
    .builder()
    .appName("poc")
    .master("local[*]")
    .config(
      "spark.sql.streaming.statefulOperator.checkCorrectness.enabled",
      "false"
    )
    .getOrCreate()

  import spark.implicits._
  val skuVmPath = Paths.get(reportsDirPath, "sku", "sku-vm.json").toString

  val vmSamplePath = Paths.get(sampleDirPath, "vm-sample.json").toString
 val df =  spark.read.option("multiline", "true").schema(
            spark.read.option("multiline", "true")
                 .json(vmSamplePath)
                 .schema
            )
            .json(skuVmPath)


import org.apache.spark.sql.functions._
val reservationTerm = "1 Year"
def reservationTermToColName(term: String): String = term.replaceAll("[^A-Za-z0-9]", "")
def reservationTermToFactor(term: String): Double = term match {
    case "1 Year" => 12.0
    case "3 Years" => 36.0
    case _      => 12.0
  }

def calculateMonthlyCost(column: Column, factor: Double, op: (Column, Double) => Column): Column =
    round(op(column, factor), 2)

 def basePricingFilter(pricingType: String, location: String): Column = {
    !col("skuName").contains("Spot") &&
    !col("skuName").contains("Low Priority") &&
    col("unitOfMeasure") === "1 Hour" &&
    col("location") === location &&
    col("type") === pricingType
  }

val computeVMPath = Paths.get(pricingDirPath, "SQL_VM_Compute.json").toString
  val rawDF = JsonReader(computeVMPath, spark).read()
  val pricingDf = rawDF.selectExpr("explode(Content) as Content").select("Content.*")

//   val storageDBPath = Paths.get(pricingDirPath, "SQL_MI_Storage.json").toString
//   val storageRawDF = JsonReader(storageDBPath, spark).read()
//   val storageDf = storageRawDF.selectExpr("explode(Content) as Content").select("Content.*")

val minPricingDf = pricingDf
      .filter(basePricingFilter("Reservation", "US West") &&
        col("reservationTerm") === reservationTerm
      )
      .groupBy("armSkuName")
      .agg(min("retailPrice").alias(s"minRetailPrice_$reservationTerm"))

val pricingMapExprEntries = minPricingDf
  .select(
    col("armSkuName").as("key"),
    col(s"minRetailPrice_$reservationTerm").as("value")
  )
  .collect()
  .flatMap { row =>
    Seq(lit(row.getString(0)), lit(row.getDouble(1)))
  }

// Build the map expression
val pricingExpr = map(pricingMapExprEntries: _*)

// ASP one
val label = "ASPProd"

val filteredASPPricing = pricingDf
      .filter(basePricingFilter(PricingType.Consumption.toString, "US West") &&
        col("savingsPlan").isNotNull
      )
      .withColumn("savingsPlan", explode(col("savingsPlan")))
      .filter(col("savingsPlan.term") === lit(reservationTerm))
      .groupBy("armSkuName")
      .agg(min("savingsPlan.retailPrice").alias(s"minRetailPrice_${label}_$reservationTerm"))

// val otDF = filteredASPPricing.filter(col("armSkuName") === "Standard_D2as_v4")
// otDF.show(false)

// filteredASPPricing.filter(col("minRetailPrice_ASPProd_1 Year") === 577.0).show(false)

val aspPricingMapExprEntries = filteredASPPricing
  .select(
    col("armSkuName").as("key"),
    col(s"minRetailPrice_${label}_$reservationTerm").as("value")
  )
  .collect()
  .flatMap { row =>
    Seq(lit(row.getString(0)), lit(row.getDouble(1)))
  }

val aspPricingExpr = map(aspPricingMapExprEntries: _*)


 val updatedDf = df.withColumn(
  "SkuRecommendationForServers",
  transform(col("SkuRecommendationForServers"), server =>
    server.withField(
      "SkuRecommendationResults",
      transform(server.getField("SkuRecommendationResults"), result =>
        result
          .withField("armSkuName", result.getField("TargetSku").getField("VirtualMachineSize").getField("AzureSkuName"))
          .withField("disks",
            flatten(array(
              coalesce(result.getField("TargetSku").getField("DataDiskSizes"), array()),
              coalesce(result.getField("TargetSku").getField("LogDiskSizes"), array()),
              coalesce(result.getField("TargetSku").getField("TempDbDiskSizes"), array())
            ))
          )
      ))))

  val outputDf = updatedDf.withColumn(
  "SkuRecommendationForServers",
  transform(col("SkuRecommendationForServers"), server =>
    server.withField(
      "SkuRecommendationResults",
      transform(server.getField("SkuRecommendationResults"), result =>
        result.withField(
            s"computeCostRI_${reservationTermToColName(reservationTerm)}",
            calculateMonthlyCost(
            element_at(
              pricingExpr,
              concat_ws("|", result.getField("armSkuName"))
            ),
            reservationTermToFactor(reservationTerm),
            _ / _
            )
        )
        .withField(
            s"computeCost${label}_${reservationTermToColName(reservationTerm)}",
            calculateMonthlyCost(
                element_at(
                    aspPricingExpr,
                    concat_ws("|", result.getField("armSkuName"))
                ),
               24 * 30.5,
               _ * _
            )
        )
      )
    )))

    
  val resultDf = outputDf.withColumn("SkuRecommendationForServers", explode(col("SkuRecommendationForServers")))
      .withColumn("SkuRecommendationResults", explode(col("SkuRecommendationForServers.SkuRecommendationResults")))
      .select("SkuRecommendationResults")
  outputDf.printSchema()
 
 outputDf.select("TimeCreated").show(false)
 
  val selectedDf = resultDf.select(
  col("SkuRecommendationResults.armSkuName"),
  col("SkuRecommendationResults.computeCostRI_1Year").as("computeCostRI_1Yr"),
  col("SkuRecommendationResults.computeCostASPProd_1Year").as("computeCostASPProd_1Year"),
)

selectedDf.show()
}
