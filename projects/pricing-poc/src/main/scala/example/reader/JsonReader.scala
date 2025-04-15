package example.reader

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

class JsonReader(filePath: String, spark: SparkSession) extends DataReader {
  override def read(): DataFrame = {
    spark.read.option("multiline", "true").json(filePath)
  }
}

object JsonReader{
  def apply(filePath: String, spark: SparkSession): JsonReader = {
    new JsonReader(filePath, spark)
  }
}