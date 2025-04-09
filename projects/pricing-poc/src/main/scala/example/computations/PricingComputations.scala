// package example.calculations

// import org.apache.spark.sql.types._
// import org.apache.spark.sql.functions._
// import org.apache.spark.sql.{DataFrame, SparkSession}
// import example.reader.JsonReader
// import example.constants.PlatformType
// import java.nio.file.Paths
// // import example.strategy.PricingStrategyFactory
// import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames

// object PricingComputations {

//   private val schema: StructType = StructType(
//     Seq(
//       StructField("keyName", StringType, nullable = false),
//       StructField("keyValue", StructType(
//           Seq(
//             StructField("computeCost", DoubleType, nullable = false),
//             StructField("storageCost", DoubleType, nullable = false),
//             StructField("iopsCost", DoubleType, nullable = false)
//           )
//         ),
//         nullable = false
//       )
//     )
//   )

//   private val pricingDataForSqlVM: Seq[(String, (Double, Double, Double))] = Seq(
//     ("With1YearASPAndDevTest", (245.13, 0.18, 0.0)),
//     ("With3YearASPAndDevTest", (24.13, 0.18, 0.0)),
//     ("With1YearASPAndProd", (242.13, 0.18, 0.0)),
//     ("With3YearASPAndProd", (222.13, 0.18, 0.0)),
//     ("With1YearRIAndDevTest", (245.13, 0.18, 0.0)),
//     ("With3YearRIAndDevTest", (24.13, 0.18, 0.0)),
//     ("With1YearRIAndProd", (242.13, 0.18, 0.0)),
//     ("With3YearRIAndProd", (222.13, 0.18, 0.0))
//   )

//   // private def generatePricingValues(
//   //     platformDf: DataFrame,
//   //     computePricingDf: DataFrame,
//   //     storagePricingDf: DataFrame,
//   //     reservationTerm: String,
//   //     environment: String,
//   //     pricingModel: String,
//   //     pricingType: String
//   // )(implicit spark: SparkSession): DataFrame = {

//   //   // Select the right strategy dynamically
//   //   val strategy = PricingStrategyFactory.getStrategy(pricingModel, pricingType, environment, reservationTerm)

//   //   // Compute cost
//   //   val computeCostDF = strategy.computeCost(platformDf, computePricingDf)
//   //   val storageCostDF = strategy.storageCost(platformDf, storagePricingDf)

//   //   val finalDF = computeCostDF
//   //     .select(coalesce(first("computeCost", ignoreNulls = true), lit(0.0)).alias("computeCost"))
//   //     .crossJoin(storageCostDF.select(coalesce(first("storageCost", ignoreNulls = true), lit(0.0)).alias("storageCost")))
//   //     .withColumn("iopsCost", lit(0.0)) 

//   //   // finalDF.show(false)
//   //   finalDF
//   // }

//   private def structurePricingData(pricingDFs: Seq[DataFrame]): DataFrame = {
//     val resultDF = pricingDFs.reduce(_.unionByName(_))
//     .select(
//       col("keyName"),
//       struct(
//         col("computeCost"),
//         col("storageCost"),
//         col("iopsCost")
//       ).alias("keyValue")
//     )

//     // resultDF.printSchema()
//     // resultDF.show(false)
//     resultDF
//   }

//   def computePricingForSqlDB(df: DataFrame): DataFrame = {
//     implicit val spark: SparkSession = df.sparkSession
//     val pricingDataFrames = loadPricingDataFrames(PlatformType.AzureSqlDatabase)
//     val computeDataFrame = pricingDataFrames.get("Compute").getOrElse(throw new RuntimeException(s"Compute pricing data not found"))
//     val storageDataFrame = loadPricingDataFrames(PlatformType.AzureSqlManagedInstance).get("Storage").getOrElse(throw new RuntimeException(s"Storage pricing data not found"))
    
//     val pricingConfigs = Seq(
//       ("With1YearRIAndProd", "1 Year", "RI", "Prod"),
//       ("With3YearRIAndProd", "3 Years", "RI", "Prod")
//     )

//     // Generate pricing DataFrames dynamically
//     val pricingDFs = pricingConfigs.map { case (keyName, term, pricingType, environment) =>
//       generatePricingValues(df, computeDataFrame, storageDataFrame, term, environment, "PaaS", pricingType)
//         .withColumn("keyName", lit(keyName))
//     }

//     structurePricingData(pricingDFs)
//   }


//   def computePricingForSqlMI(df: DataFrame): DataFrame = {
//     implicit val spark: SparkSession = df.sparkSession
//     val pricingDataFrames = loadPricingDataFrames(PlatformType.AzureSqlManagedInstance)
//     val computeDataFrame = pricingDataFrames.get("Compute").getOrElse(throw new RuntimeException(s"Compute pricing data not found"))
//     val storageDataFrame = pricingDataFrames.get("Storage").getOrElse(throw new RuntimeException(s"Storage pricing data not found"))
    
//     val pricingConfigs = Seq(
//       ("With1YearRIAndProd", "1 Year", "RI", "Prod"),
//       ("With3YearRIAndProd", "3 Years", "RI", "Prod")
//     )

//     // Generate pricing DataFrames dynamically
//     val pricingDFs = pricingConfigs.map { case (keyName, term, pricingType, environment) =>
//       generatePricingValues(df, computeDataFrame, storageDataFrame, term, environment, "PaaS", pricingType)
//         .withColumn("keyName", lit(keyName))
//     }

//     structurePricingData(pricingDFs)
//   }

//   def computePricingForSqlVM(df: DataFrame): DataFrame = {
//     implicit val spark: SparkSession = df.sparkSession
//     val pricingDataFrames = loadPricingDataFrames(PlatformType.AzureSqlVirtualMachine)
//     val computeDataFrame = pricingDataFrames.get("Compute").getOrElse(throw new RuntimeException(s"Compute pricing data not found"))
//     val storageDataFrame = pricingDataFrames.get("Storage").getOrElse(throw new RuntimeException(s"Storage pricing data not found"))

//     val pricingConfigs = Seq(
//       ("With1YearASPAndDevTest", "1 Year", "ASP", "DevTest"),
//       ("With3YearASPAndDevTest", "3 Years", "ASP", "DevTest"),
//       ("With1YearASPAndProd", "1 Year", "ASP", "Prod"),
//       ("With3YearASPAndProd", "3 Years", "ASP", "Prod"),
//       ("With1YearRIAndProd", "1 Year", "RI", "Prod"),
//       ("With3YearRIAndProd", "3 Years", "RI", "Prod")
//     )

//     // Generate pricing DataFrames dynamically
//     val pricingDFs = pricingConfigs.map { case (keyName, term, pricingType, environment) =>
//       generatePricingValues(df, computeDataFrame, storageDataFrame, term, environment, "IaaS", pricingType)
//         .withColumn("keyName", lit(keyName))
//     }

//     structurePricingData(pricingDFs)

//     // val pricingData: Seq[(String, (Double, Double, Double))] = Seq(
//     //   ("With1YearASPAndDevTest", (245.13, 0.18, 0.0)),
//     //   ("With3YearASPAndDevTest", (24.13, 0.18, 0.0)),
//     //   ("With1YearASPAndProd", (242.13, 0.18, 0.0)),
//     //   ("With3YearASPAndProd", (222.13, 0.18, 0.0)),
//     //   ("With1YearRIAndDevTest", (245.13, 0.18, 0.0)),
//     //   ("With3YearRIAndDevTest", (24.13, 0.18, 0.0)),
//     //   ("With1YearRIAndProd", generatePricingValues(df, computeDataFrame, storageDataFrame, "1 Year", "Prod", "IaaS", "RI")),
//     //   ("With3YearRIAndProd", generatePricingValues(df, computeDataFrame, storageDataFrame, "3 Years", "Prod", "IaaS", "RI"))
//     // )

//     // structurePricingData(df, pricingData)
//   }

//   def loadPricingDataFrames(platformType: PlatformType)(implicit spark: SparkSession): Map[String, DataFrame] = {
//     val fileMappings: Map[PlatformType, Map[String, String]] = Map(
//       PlatformType.AzureSqlDatabase -> Map(
//         "Compute" -> "SQL_DB_Compute.json",
//         // "License" -> "SQL_DB_License.json",
//         "Storage" -> "SQL_DB_Storage.json"
//       ),
//       PlatformType.AzureSqlManagedInstance -> Map(
//         "Compute" -> "SQL_MI_Compute.json",
//         // "License" -> "SQL_MI_License.json",
//         "Storage" -> "SQL_MI_Storage.json"
//       ),
//       PlatformType.AzureSqlVirtualMachine -> Map(
//         "Compute" -> "SQL_VM_Compute.json",
//         "Storage" -> "SQL_VM_Storage.json"
//       )
//     )

//     val pricingDataFolderPath = Paths.get(System.getProperty("user.dir"), "src", "main", "resources", "pricing").toString
//     val fileMapping = fileMappings.getOrElse(platformType, Map.empty)

//     fileMapping.map { case (category, fileName) =>
//       val filePath = s"$pricingDataFolderPath/$fileName"
//       val rawDF = JsonReader.readJson(spark, filePath)
//       val contentDF = rawDF.selectExpr("explode(Content) as Content").select("Content.*")

//       category -> contentDF
//     }
//   }
// }
