package example.reader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

object JsonReader {
  def readJson(spark: SparkSession, filePath: String): DataFrame = {
    spark.read.option("multiline", "true").json(filePath)
  }

  def readJsonWithSchema(
      spark: SparkSession,
      filePath: String,
      schema: StructType
  ) = {
    spark.read.option("multiline", "true").schema(schema).json(filePath)
  }

  def readJsonWithSchemaInferred(
      spark: SparkSession,
      payloadFilePath: String,
      schemaSampleFilePath: String
  ): DataFrame = spark.read.option("multiline", "true")
                      .schema(
                        spark.read.option("multiline", "true")
                             .json(schemaSampleFilePath)
                             .schema
                       )
                      .json(payloadFilePath)
}
