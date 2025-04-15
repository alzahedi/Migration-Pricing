package example.transformers

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import example.constants.MigrationAssessmentSourceTypes
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

class MigrationAssessmentTransformer(resourceType: MigrationAssessmentSourceTypes.Value
) extends DataTransformer {

  override def transform(df: DataFrame): DataFrame = {
    resourceType match {
        case MigrationAssessmentSourceTypes.EventHubRawEventStream => processRawEventHubStream(df)
    }
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
}

object MigrationAssessmentTransformer{
    def apply(resourceType: MigrationAssessmentSourceTypes.Value): MigrationAssessmentTransformer ={
        new MigrationAssessmentTransformer(resourceType)
    }
}
