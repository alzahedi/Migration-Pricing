package example.reader

import org.apache.spark.sql.{DataFrame, SparkSession}
import example.constants.MigrationAssessmentSourceTypes
import example.eventhub.{ReadEventHubFeedFormat, EventHubStreamFeed}

class MigrationAssessmentReader(
    spark: SparkSession,
    eventHubFeedFormat: ReadEventHubFeedFormat,
    resourceType: MigrationAssessmentSourceTypes.Value
) extends DataReader {

  override def read(): DataFrame = {
    resourceType match {
      case MigrationAssessmentSourceTypes.EventHubRawEventStream =>
        EventHubStreamFeed(eventHubFeedFormat, spark).read()
    }
  }
}

object MigrationAssessmentReader {
  def apply(
      spark: SparkSession,
      eventHubFeedFormat: ReadEventHubFeedFormat,
      resourceType: MigrationAssessmentSourceTypes.Value
  ): MigrationAssessmentReader = {
    new MigrationAssessmentReader(spark, eventHubFeedFormat, resourceType)
  }
}
