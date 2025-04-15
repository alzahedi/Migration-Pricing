package example.transformers

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import example.constants.MigrationAssessmentSourceTypes
import example.constants.MigrationAssessmentConstants
import example.reader.JsonReader
import java.nio.file.Paths
import example.constants.PlatformType
import example.computations.{
  PricingComputation,
  SqlDbPricingComputation,
  SqlMiPricingComputation,
  SqlVmPricingComputation
}

class MigrationAssessmentTransformer(
    resourceType: MigrationAssessmentSourceTypes.Value,
    spark: SparkSession,
    platformType: PlatformType = null,
    skuDbDF: DataFrame = null,
    skuMiDF: DataFrame = null,
    skuVmDF: DataFrame = null
) extends DataTransformer {

  override def transform(df: DataFrame): DataFrame = {
    resourceType match {
      case MigrationAssessmentSourceTypes.EventHubRawEventStream => processRawEventHubStream(df)
      case MigrationAssessmentSourceTypes.Suitability         |
           MigrationAssessmentSourceTypes.SkuRecommendationDB |
           MigrationAssessmentSourceTypes.SkuRecommendationMI | 
           MigrationAssessmentSourceTypes.SkuRecommendationVM    => processTypedEventHubStream(df)
      case MigrationAssessmentSourceTypes.PricingComputation     => processPricingComputation(df)
      case MigrationAssessmentSourceTypes.FullAssessment         => processFullAssessment(df)
    }
  }

  private def processFullAssessment(df: DataFrame): DataFrame = {
    df.as("es")
      .join(skuDbDF.as("esrasd"), expr(s"""(es.uploadIdentifier == esrasd.uploadIdentifier) AND (es.enqueuedTime BETWEEN (esrasd.enqueuedTime - ${MigrationAssessmentConstants.DefaultAcrossStreamsIntervalMaxLag}) AND (esrasd.enqueuedTime + ${MigrationAssessmentConstants.DefaultAcrossStreamsIntervalMaxLag}))"""), joinType = "inner")
      .join(skuMiDF.as("esrasm"), expr(s"""(es.uploadIdentifier == esrasm.uploadIdentifier) AND (es.enqueuedTime BETWEEN (esrasm.enqueuedTime - ${MigrationAssessmentConstants.DefaultAcrossStreamsIntervalMaxLag}) AND (esrasm.enqueuedTime + ${MigrationAssessmentConstants.DefaultAcrossStreamsIntervalMaxLag}))"""), joinType = "inner")
      .join(skuVmDF.as("esrasv"), expr(s"""(es.uploadIdentifier == esrasv.uploadIdentifier) AND (es.enqueuedTime BETWEEN (esrasv.enqueuedTime - ${MigrationAssessmentConstants.DefaultAcrossStreamsIntervalMaxLag}) AND (esrasv.enqueuedTime + ${MigrationAssessmentConstants.DefaultAcrossStreamsIntervalMaxLag}))"""), joinType = "inner")
      .drop("type")
      .drop("timestamp")
      .drop("uploadIdentifier")
  }

  private def processRawEventHubStream(df: DataFrame): DataFrame = {
    val eventSchema = StructType(Seq(
      StructField("uploadIdentifier", StringType, nullable = false),
      StructField("type", StringType, nullable = false),
      StructField("body", StringType, nullable = false)
    ))

    df
      .selectExpr("CAST(body AS STRING) AS message", "enqueuedTime")
      .select(from_json(col("message"), eventSchema).as("data"), col("enqueuedTime"))
      .select("data.*", "enqueuedTime")
      .withColumn("timestamp", current_timestamp())
  }

  private def processPricingComputation(df: DataFrame): DataFrame = {
    val computation: PricingComputation = platformType match {
      case PlatformType.AzureSqlDatabase        => new SqlDbPricingComputation(spark)
      case PlatformType.AzureSqlManagedInstance => new SqlMiPricingComputation(spark)
      case PlatformType.AzureSqlVirtualMachine  => new SqlVmPricingComputation(spark)
      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported platform: $platformType"
        )
    }
    computation.compute(df)
  }

  private def processTypedEventHubStream(df: DataFrame): DataFrame = {
    val reportsDirPath = Paths.get(System.getProperty("user.dir"), "src", "main", "resources", "reports").toString
    val jsonPaths: Map[MigrationAssessmentSourceTypes.Value, String] = Map(
      MigrationAssessmentSourceTypes.Suitability -> Paths.get(reportsDirPath, "suitability", "suit.json").toString,
      MigrationAssessmentSourceTypes.SkuRecommendationDB -> Paths.get(reportsDirPath, "sku", "sku-db.json").toString,
      MigrationAssessmentSourceTypes.SkuRecommendationMI -> Paths.get(reportsDirPath, "sku", "sku-mi.json").toString,
      MigrationAssessmentSourceTypes.SkuRecommendationVM -> Paths.get(reportsDirPath, "sku", "sku-vm.json").toString
    )
    val schema = JsonReader(jsonPaths(resourceType), spark).read().schema

    df.filter(col("type") === resourceType.toString)
        .select(col("*"), from_json(col("body"), schema).as("body_struct"))
        .drop("body")
        .select(col("*"), col("body_struct.*"))
        .drop("body_struct")
        .withWatermark("enqueuedTime", MigrationAssessmentConstants.DefaultLateArrivingWatermarkTime)
  }
}

object MigrationAssessmentTransformer {
  def apply(
    resourceType: MigrationAssessmentSourceTypes.Value, 
    spark: SparkSession, 
    platformType: PlatformType = null,
    skuDbDF: DataFrame = null,
    skuMiDF: DataFrame = null,
    skuVmDF: DataFrame = null
): MigrationAssessmentTransformer = {
    new MigrationAssessmentTransformer(resourceType, spark, platformType, skuDbDF, skuMiDF, skuVmDF)
  }
}
