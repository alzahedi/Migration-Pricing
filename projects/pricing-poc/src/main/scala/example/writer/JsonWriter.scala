package example.writer

import java.nio.file.{Files, Paths, StandardCopyOption}
import org.apache.spark.sql.DataFrame

object JsonWriter {
  def writeToJsonFile(df: DataFrame, outputPath: String): Unit = {
    val tempDir = outputPath + "_temp"

    // Write JSON output to a temporary directory
    df.coalesce(1).write.mode("overwrite").json(tempDir)

    // Move the single JSON file from the directory to the desired output.json
    val tempFile = Files
      .list(Paths.get(tempDir))
      .filter(_.getFileName.toString.endsWith(".json"))
      .findFirst()
      .orElseThrow(() => new RuntimeException("No JSON file found!"))

    Files.move(
      tempFile,
      Paths.get(outputPath),
      StandardCopyOption.REPLACE_EXISTING
    )

    // Cleanup temporary directory
    Files
      .walk(Paths.get(tempDir))
      .sorted(java.util.Comparator.reverseOrder())
      .forEach(Files.delete)

    println(s"Processed data saved to: $outputPath")
  }
}
