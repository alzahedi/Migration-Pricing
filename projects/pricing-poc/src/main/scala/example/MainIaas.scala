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
import example.constants.RecommendationConstants
import example.constants.DiskTypeToTierMap
import org.apache.spark.sql.expressions.Window

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
  val skuVmPath = Paths.get(reportsDirPath, "sku", "sku-vm-test.json").toString

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

  def getPremiumSSDV2DiskPrices(pricingDF: DataFrame): DataFrame = {
    def getMeterDF(meterName: String, meterType: String): DataFrame = {
     val result = pricingDF
        .filter(
          col("meterName") === meterName &&
          col("productName").contains(RecommendationConstants.PremiumSSDV2ProductName) &&
          col("type") === "Consumption" &&
          //lower(col("unitOfMeasure")) === "1 gib/hour" || lower(col("unitOfMeasure")) === "1/hour" &&
          col("retailPrice") > 0
        )
        .orderBy("retailPrice")
        .limit(1)
        .withColumn("meterType", lit(meterType))
        // print(meterName)
        // result.show(false)
        result
    }


    getMeterDF(RecommendationConstants.PremiumSSDV2StorageMeterName, "StoragePrice")
      .union(getMeterDF(RecommendationConstants.PremiumSSDV2IOPSMeterName, "IOPSPrice"))
      .union(getMeterDF(RecommendationConstants.PremiumSSDV2ThroughputMeterName, "ThroughputPrice"))
      .select("meterType", "retailPrice")
  }

val computeVMPath = Paths.get(pricingDirPath, "SQL_VM_Compute.json").toString
  val rawDF = JsonReader(computeVMPath, spark).read()
  val pricingDf = rawDF.selectExpr("explode(Content) as Content").select("Content.*")

  val storageDBPath = Paths.get(pricingDirPath, "SQL_VM_Storage.json").toString
  val storageRawDF = JsonReader(storageDBPath, spark).read()
  val storageDf = storageRawDF.selectExpr("explode(Content) as Content").select("Content.*")

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

// Storage calculations 

val premiumSSDV2PricesDF = getPremiumSSDV2DiskPrices(storageDf) 
//premiumSSDV2PricesDF.show(false)

val pivotedDf = premiumSSDV2PricesDF
  .groupBy() // no grouping column — aggregate all rows into one row
  .pivot("meterType", Seq("StoragePrice", "IOPSPrice", "ThroughputPrice"))
  .agg(first("retailPrice"))

// Step 2: Add derived fields using round
val enrichedDf = pivotedDf
  .withColumn("pricePerGib", round(col("StoragePrice") * 24 * 30.5, 4))
  .withColumn("pricePerIOPS", round(col("IOPSPrice") * 24 * 30.5, 4))
  .withColumn("pricePerMbps", round(col("ThroughputPrice") * 24 * 30.5, 4))

//enrichedDf.show(false)
// Step 3: Convert to Map expression — key/value pairs
val row = enrichedDf.first()

val priceMap = Map(
  "StoragePrice"   -> row.getAs[Double]("StoragePrice"),
  "IOPSPrice"      -> row.getAs[Double]("IOPSPrice"),
  "ThroughputPrice"-> row.getAs[Double]("ThroughputPrice"),
  "pricePerGib"    -> row.getAs[Double]("pricePerGib"),
  "pricePerIOPS"   -> row.getAs[Double]("pricePerIOPS"),
  "pricePerMbps"   -> row.getAs[Double]("pricePerMbps")
)

val priceMapExpr = map(
  lit("StoragePrice")    , lit(priceMap("StoragePrice")),
  lit("IOPSPrice")       , lit(priceMap("IOPSPrice")),
  lit("ThroughputPrice") , lit(priceMap("ThroughputPrice")),
  lit("pricePerGib")     , lit(priceMap("pricePerGib")),
  lit("pricePerIOPS")    , lit(priceMap("pricePerIOPS")),
  lit("pricePerMbps")    , lit(priceMap("pricePerMbps"))
)

val diskPricingFiltered = storageDf
      .filter(col("type") === "Consumption" && col("unitOfMeasure") === "1/Month" && !col("meterName").contains("Free"))
      .withColumn(
  "mappedProductName",
  when(col("productName").contains("Standard"), lit("Standard"))
 .when(col("productName").contains("Premium"), lit("Premium"))
 .when(col("productName").contains("Ultra"), lit("Ultra"))
    .otherwise(lit("Other")) // Default to "Other" if neither condition is met
).filter(col("mappedProductName") !== "Other")

val groupedPricing = diskPricingFiltered
  .groupBy("meterName", "mappedProductName")
  .agg(
    min("retailPrice").as("minRetailPrice")
  )

// groupedPricing.filter(col("meterName") === "P2 LRS Disk").show(false)

//diskPricingFiltered.select("productName", "mappedProductName", "meterName", "retailPrice").show(false)

val diskMapEntries = groupedPricing
  .select(
    concat_ws("|", col("meterName"), col("mappedProductName")).as("key"),
    col("minRetailPrice").as("value")
  )
  .collect()
  .flatMap(row => Seq(lit(row.getString(0)), lit(row.getDouble(1))))

val diskPriceExpr = map(diskMapEntries: _*)

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


val enrichedWithDiskTransform = updatedDf.withColumn(
     "SkuRecommendationForServers",
      transform(col("SkuRecommendationForServers"), server =>
      server.withField(
        "SkuRecommendationResults",
        transform(server.getField("SkuRecommendationResults"), sku =>
          sku.withField(
            "disks",
            transform(sku.getField("disks"), result =>
                result.withField("DiskMeter",
                    when(result.getField("Type") === "PremiumSSD", concat(result.getField("Size"), lit(" LRS Disk")))
                    .otherwise(concat(result.getField("Size"), lit(" Disks")))
                )
                .withField("DiskProductName",
                    when(result.getField("Type").isin("StandardHDD", "StandardSSD"), "Standard")
                    when(result.getField("Type").isin("PremiumSSD", "PremiumSSDV2"), "Premium")
                    when(result.getField("Type").isin("UltraSSD"), "Ultra")
                )
            )
          )
        )))
    )

 val enrichedWithSSDV2 = enrichedWithDiskTransform.withColumn(
     "SkuRecommendationForServers",
      transform(col("SkuRecommendationForServers"), server =>
      server.withField(
        "SkuRecommendationResults",
        transform(server.getField("SkuRecommendationResults"), sku =>
          sku.withField(
            "disks",
            transform(sku.getField("disks"), result =>
                result.withField("PremiumSSDV2StorageCost",
                    when(result.getField("Type") === "PremiumSSDV2", element_at(priceMapExpr, lit("pricePerGib")) * result.getField("MaxSizeInGib")).otherwise(0.0)
                )
                .withField("PremiumSSDV2IOPSCost",
                    when(result.getField("Type") === "PremiumSSDV2", element_at(priceMapExpr, lit("pricePerIOPS")) * result.getField("MaxIOPS") - 3000).otherwise(0.0)
                )
                .withField("PremiumSSDV2ThroughputCost",
                    when(result.getField("Type") === "PremiumSSDV2", element_at(priceMapExpr, lit("pricePerMbps")) * result.getField("MaxThroughputInMbps") - 125).otherwise(0.0)
                )
            )
          )
        )))
    )

val enrichedWithSSDV2TotalCost = enrichedWithSSDV2.withColumn(
     "SkuRecommendationForServers",
      transform(col("SkuRecommendationForServers"), server =>
      server.withField(
        "SkuRecommendationResults",
        transform(server.getField("SkuRecommendationResults"), sku =>
          sku.withField(
            "disks",
            transform(sku.getField("disks"), result =>
                result.withField("TotalPremiumSSDV2Cost",
                    when(result.getField("Type") === "PremiumSSDV2", result.getField("PremiumSSDV2StorageCost") +  result.getField("PremiumSSDV2IOPSCost") +  result.getField("PremiumSSDV2ThroughputCost")).otherwise(0.0)
                )
            )
          )
        )))
    )

val enrichedWithSSDCost = enrichedWithSSDV2TotalCost.withColumn(
     "SkuRecommendationForServers",
      transform(col("SkuRecommendationForServers"), server =>
      server.withField(
        "SkuRecommendationResults",
        transform(server.getField("SkuRecommendationResults"), sku =>
          sku.withField(
            "disks",
            transform(sku.getField("disks"), result =>
               result.withField("TotalPremiumSSDCost",
                when(result.getField("Type") === "PremiumSSD", element_at(
                    diskPriceExpr,
                    concat_ws("|", result.getField("DiskMeter"), result.getField("DiskProductName"))
                )).otherwise(0.0)
               )
               .withField("TotalOtherDiskCost",
               when(result.getField("Type") =!= "PremiumSSD" && result.getField("Type") =!= "PremiumSSDV2", element_at(
                    diskPriceExpr,
                    concat_ws("|", result.getField("DiskMeter"), result.getField("DiskProductName"))
                )).otherwise(0.0)
               )
            )
          )
        )))
    )

  val outputDf = enrichedWithSSDCost.withColumn(
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
        .withField(
          // Add the StorageCost field by summing the disk costs for all disks
          "StorageCost",
          // Aggregate the disk costs in the disks array
          aggregate(
            result.getField("disks"),
            lit(0).cast("double"),
            (acc, disk) => acc + coalesce(disk.getField("TotalPremiumSSDV2Cost"), lit(0.0)) +
                         coalesce(disk.getField("TotalPremiumSSDCost"), lit(0.0)) +
                         coalesce(disk.getField("TotalOtherDiskCost"), lit(0.0)),
            acc => acc
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
//   col("SkuRecommendationResults.disks"),
  col("SkuRecommendationResults.computeCostRI_1Year").as("computeCostRI_1Yr"),
  col("SkuRecommendationResults.computeCostASPProd_1Year").as("computeCostASPProd_1Year"),
  col("SkuRecommendationResults.StorageCost")
)

selectedDf.show(false)



}
