package example.schema

import org.apache.spark.sql.types._

object Schema {
  val skuSchema = StructType(
    Seq(
      StructField("TimeCreated", StringType, nullable = false),
      StructField(
        "SkuRecommendationForServers",
        ArrayType(
          StructType(
            Seq(
              StructField("ServerName", StringType, nullable = false),
              StructField("TargetPlatform", StringType, nullable = false),
              StructField(
                "Requirements",
                StructType(
                  Seq(
                    StructField(
                      "CpuRequirementInCores",
                      DoubleType,
                      nullable = false
                    ),
                    StructField(
                      "DataStorageRequirementInMB",
                      DoubleType,
                      nullable = false
                    ),
                    StructField(
                      "LogStorageRequirementInMB",
                      DoubleType,
                      nullable = false
                    ),
                    StructField(
                      "MemoryRequirementInMB",
                      DoubleType,
                      nullable = false
                    ),
                    StructField(
                      "InstanceIOPSDataLogFileRequirement",
                      DoubleType,
                      nullable = false
                    ),
                    StructField(
                      "DataIOPSRequirement",
                      DoubleType,
                      nullable = false
                    ),
                    StructField(
                      "LogIOPSRequirement",
                      DoubleType,
                      nullable = false
                    ),
                    StructField(
                      "IOLatencyRequirementInMs",
                      DoubleType,
                      nullable = false
                    ),
                    StructField(
                      "IOThroughputRequirementInMBps",
                      DoubleType,
                      nullable = false
                    ),
                    StructField(
                      "DataIOThroughputRequirementInMBps",
                      DoubleType,
                      nullable = false
                    ),
                    StructField(
                      "LogIOThroughputRequirementInMBps",
                      DoubleType,
                      nullable = false
                    ),
                    StructField("InstanceId", StringType, nullable = false),
                    StructField(
                      "InstanceVersion",
                      StringType,
                      nullable = false
                    ),
                    StructField(
                      "InstanceEdition",
                      StringType,
                      nullable = false
                    ),
                    StructField(
                      "LogicalCpuCount",
                      IntegerType,
                      nullable = false
                    ),
                    StructField(
                      "ServerCollation",
                      StringType,
                      nullable = false
                    ),
                    StructField(
                      "HyperthreadRatio",
                      DoubleType,
                      nullable = false
                    ),
                    StructField(
                      "PhysicalCpuCount",
                      DoubleType,
                      nullable = false
                    ),
                    StructField(
                      "MaxServerMemoryInUse",
                      DoubleType,
                      nullable = false
                    ),
                    StructField("TempDBSizeInMB", DoubleType, nullable = false),
                    StructField("NumOfLogins", IntegerType, nullable = false),
                    StructField("SqlStartTime", StringType, nullable = false),
                    StructField(
                      "AggregatedPhysicalMemoryInUse",
                      DoubleType,
                      nullable = false
                    ),
                    StructField(
                      "AggregatedMemoryUtilizationPercentage",
                      DoubleType,
                      nullable = false
                    ),
                    StructField(
                      "AggregatedSqlInstanceCpuPercent",
                      DoubleType,
                      nullable = false
                    ),
                    StructField(
                      "DataPointsStartTime",
                      StringType,
                      nullable = false
                    ),
                    StructField(
                      "DataPointsEndTime",
                      StringType,
                      nullable = false
                    ),
                    StructField(
                      "AggregationTargetPercentile",
                      IntegerType,
                      nullable = false
                    ),
                    StructField(
                      "PerfDataCollectionIntervalInSeconds",
                      IntegerType,
                      nullable = false
                    ),
                    StructField(
                      "SqlServerHostRequirements",
                      StructType(
                        Seq(
                          StructField(
                            "HostNICCount",
                            IntegerType,
                            nullable = false
                          )
                        )
                      ),
                      nullable = false
                    ),
                    StructField(
                      "IsSqlServerFci",
                      BooleanType,
                      nullable = false
                    ),
                    StructField(
                      "IsSqlServerHostingAvailabilityGroup",
                      BooleanType,
                      nullable = false
                    ),
                    StructField(
                      "DatabaseLevelRequirements",
                      ArrayType(
                        StructType(
                          Seq(
                            StructField(
                              "DatabaseCollation",
                              StringType,
                              nullable = false
                            ),
                            StructField(
                              "CpuRequirementInCores",
                              DoubleType,
                              nullable = false
                            ),
                            StructField(
                              "CpuRequirementInPercentageOfTotalInstance",
                              DoubleType,
                              nullable = false
                            ),
                            StructField(
                              "MemoryRequirementInMB",
                              DoubleType,
                              nullable = false
                            ),
                            StructField(
                              "DataIOPSRequirement",
                              DoubleType,
                              nullable = false
                            ),
                            StructField(
                              "LogIOPSRequirement",
                              DoubleType,
                              nullable = false
                            ),
                            StructField(
                              "IOLatencyRequirementInMs",
                              DoubleType,
                              nullable = false
                            ),
                            StructField(
                              "IOThroughputRequirementInMBps",
                              DoubleType,
                              nullable = false
                            ),
                            StructField(
                              "DataFileIOThroughputRequirementInMBps",
                              DoubleType,
                              nullable = false
                            ),
                            StructField(
                              "LogFileIOThroughputRequirementInMBps",
                              DoubleType,
                              nullable = false
                            ),
                            StructField(
                              "LogWriteRateInMBPerSecRequirement",
                              DoubleType,
                              nullable = false
                            ),
                            StructField(
                              "DataStorageRequirementInMB",
                              DoubleType,
                              nullable = false
                            ),
                            StructField(
                              "LogStorageRequirementInMB",
                              DoubleType,
                              nullable = false
                            ),
                            StructField(
                              "DatabaseName",
                              StringType,
                              nullable = false
                            ),
                            StructField(
                              "AggregatedCachedSizeInMb",
                              DoubleType,
                              nullable = false
                            ),
                            StructField(
                              "AggregatedCpuPercent",
                              DoubleType,
                              nullable = false
                            ),
                            StructField(
                              "ExcludeFromRecommendation",
                              BooleanType,
                              nullable = false
                            ),
                            StructField(
                              "FileLevelRequirements",
                              ArrayType(
                                StructType(
                                  Seq(
                                    StructField(
                                      "ParentDatabaseName",
                                      StringType,
                                      nullable = false
                                    ),
                                    StructField(
                                      "FileLogicalName",
                                      StringType,
                                      nullable = false
                                    ),
                                    StructField(
                                      "FileName",
                                      StringType,
                                      nullable = false
                                    ),
                                    StructField(
                                      "FileType",
                                      StringType,
                                      nullable = false
                                    ),
                                    StructField(
                                      "SizeInMB",
                                      DoubleType,
                                      nullable = false
                                    ),
                                    StructField(
                                      "ReadLatencyInMs",
                                      DoubleType,
                                      nullable = false
                                    ),
                                    StructField(
                                      "WriteLatencyInMs",
                                      DoubleType,
                                      nullable = false
                                    ),
                                    StructField(
                                      "AggregatedNumOfReads",
                                      DoubleType,
                                      nullable = false
                                    ),
                                    StructField(
                                      "AggregatedNumOfWrites",
                                      DoubleType,
                                      nullable = false
                                    ),
                                    StructField(
                                      "AggregatedReadIOInMb",
                                      DoubleType,
                                      nullable = false
                                    ),
                                    StructField(
                                      "AggregatedWriteIOInMb",
                                      DoubleType,
                                      nullable = false
                                    ),
                                    StructField(
                                      "CollectionInterval",
                                      DoubleType,
                                      nullable = false
                                    ),
                                    StructField(
                                      "IOPSRequirement",
                                      DoubleType,
                                      nullable = false
                                    ),
                                    StructField(
                                      "IOThroughputRequirementInMBps",
                                      DoubleType,
                                      nullable = false
                                    ),
                                    StructField(
                                      "WriteRateRequirementInMBps",
                                      DoubleType,
                                      nullable = false
                                    ),
                                    StructField(
                                      "NumberOfDataPointsAnalyzed",
                                      DoubleType,
                                      nullable = false
                                    )
                                  )
                                )
                              ),
                              nullable = false
                            ),
                            StructField(
                              "NumberOfDataPointsAnalyzed",
                              DoubleType,
                              nullable = false
                            )
                          )
                        )
                      ),
                      nullable = false
                    ),
                    StructField(
                      "NumberOfDataPointsAnalyzed",
                      DoubleType,
                      nullable = false
                    )
                  )
                ),
                nullable = false
              ),
              StructField(
                "SkuRecommendationResults",
                ArrayType(
                  StructType(
                    Seq(
                      StructField(
                        "SqlInstanceName",
                        StringType,
                        nullable = false
                      ),
                      StructField(
                        "ServerCollation",
                        StringType,
                        nullable = false
                      ),
                      StructField("DatabaseName", StringType, nullable = false),
                      StructField(
                        "DatabaseCollation",
                        StringType,
                        nullable = false
                      ),
                      StructField(
                        "TargetSku",
                        StructType(
                          Seq(
                            StructField(
                              "StorageMaxSizeInMb",
                              DoubleType,
                              nullable = false
                            ),
                            StructField(
                              "PredictedDataSizeInMb",
                              DoubleType,
                              nullable = false
                            ),
                            StructField(
                              "PredictedLogSizeInMb",
                              DoubleType,
                              nullable = false
                            ),
                            StructField(
                              "MaxStorageIops",
                              DoubleType,
                              nullable = false
                            ),
                            StructField(
                              "MaxThroughputMBps",
                              DoubleType,
                              nullable = false
                            ),
                            StructField(
                              "Category",
                              StructType(
                                Seq(
                                  StructField(
                                    "SqlPurchasingModel",
                                    StringType,
                                    nullable = false
                                  ),
                                  StructField(
                                    "SqlServiceTier",
                                    StringType,
                                    nullable = false
                                  ),
                                  StructField(
                                    "ComputeTier",
                                    StringType,
                                    nullable = false
                                  ),
                                  StructField(
                                    "HardwareType",
                                    StringType,
                                    nullable = false
                                  ),
                                  StructField(
                                    "ZoneRedundancyAvailable",
                                    BooleanType,
                                    nullable = false
                                  ),
                                  StructField(
                                    "SqlTargetPlatform",
                                    StringType,
                                    nullable = false
                                  )
                                )
                              ),
                              nullable = false
                            ),
                            StructField(
                              "ComputeSize",
                              IntegerType,
                              nullable = false
                            )
                          )
                        ),
                        nullable = false
                      ),
                      StructField(
                        "MonthlyCost",
                        StructType(
                          Seq(
                            StructField(
                              "ComputeCost",
                              DoubleType,
                              nullable = false
                            ),
                            StructField(
                              "StorageCost",
                              DoubleType,
                              nullable = false
                            ),
                            StructField(
                              "IopsCost",
                              DoubleType,
                              nullable = false
                            ),
                            StructField(
                              "TotalCost",
                              DoubleType,
                              nullable = false
                            )
                          )
                        ),
                        nullable = false
                      ),
                      StructField("Ranking", IntegerType, nullable = false),
                      StructField(
                        "RecommendationReasonings",
                        ArrayType(
                          StructType(
                            Seq(
                              StructField(
                                "ReasoningType",
                                StringType,
                                nullable = false
                              ),
                              StructField(
                                "ReasoningEnum",
                                StringType,
                                nullable = false
                              ),
                              StructField(
                                "ReasoningString",
                                StringType,
                                nullable = false
                              ),
                              StructField(
                                "ReasoningCategory",
                                StringType,
                                nullable = false
                              ),
                              StructField(
                                "Parameters",
                                MapType(StringType, StringType),
                                nullable = false
                              )
                            )
                          )
                        ),
                        nullable = false
                      ),
                      StructField(
                        "PositiveJustifications",
                        ArrayType(StringType),
                        nullable = false
                      ),
                      StructField(
                        "NegativeJustifications",
                        ArrayType(StringType),
                        nullable = false
                      )
                    )
                  )
                ),
                nullable = false
              )
            )
          )
        ),
        nullable = false
      )
    )
  )

}
