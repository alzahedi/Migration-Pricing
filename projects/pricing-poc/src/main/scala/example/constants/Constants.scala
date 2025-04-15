package example.constants

sealed trait PlatformType
sealed trait PricingModel
sealed trait PricingType

object PlatformType {
  case object AzureSqlDatabase extends PlatformType
  case object AzureSqlManagedInstance extends PlatformType
  case object AzureSqlVirtualMachine extends PlatformType

  def values: List[PlatformType] = List(AzureSqlDatabase, AzureSqlManagedInstance, AzureSqlVirtualMachine)

  def fromString(value: String): Option[PlatformType] = value match {
    case "AzureSqlDatabase"        => Some(AzureSqlDatabase)
    case "AzureSqlManagedInstance" => Some(AzureSqlManagedInstance)
    case "AzureSqlVirtualMachine"  => Some(AzureSqlVirtualMachine)
    case _                         => None
  }

  def toString(platformType: PlatformType): String = platformType match {
    case AzureSqlDatabase        => "AzureSqlDatabase"
    case AzureSqlManagedInstance => "AzureSqlManagedInstance"
    case AzureSqlVirtualMachine  => "AzureSqlVirtualMachine"
  }
}


object PricingModel {
  case object With1YearASPAndDevTest extends PricingModel
  case object With3YearASPAndDevTest extends PricingModel
  case object With1YearASPAndProd extends PricingModel
  case object With3YearASPAndProd extends PricingModel
  case object With1YearRIAndDevTest extends PricingModel
  case object With3YearRIAndDevTest extends PricingModel
  case object With1YearRIAndProd extends PricingModel
  case object With3YearRIAndProd extends PricingModel

  def values: List[PricingModel] = List(
    With1YearASPAndDevTest, With3YearASPAndDevTest,
    With1YearASPAndProd, With3YearASPAndProd,
    With1YearRIAndDevTest, With3YearRIAndDevTest,
    With1YearRIAndProd, With3YearRIAndProd
  )

  def fromString(value: String): Option[PricingModel] = value match {
    case "With1YearASPAndDevTest" => Some(With1YearASPAndDevTest)
    case "With3YearASPAndDevTest" => Some(With3YearASPAndDevTest)
    case "With1YearASPAndProd"    => Some(With1YearASPAndProd)
    case "With3YearASPAndProd"    => Some(With3YearASPAndProd)
    case "With1YearRIAndDevTest"  => Some(With1YearRIAndDevTest)
    case "With3YearRIAndDevTest"  => Some(With3YearRIAndDevTest)
    case "With1YearRIAndProd"     => Some(With1YearRIAndProd)
    case "With3YearRIAndProd"     => Some(With3YearRIAndProd)
    case _                        => None
  }

  def toString(pricingType: PricingModel): String = pricingType match {
    case With1YearASPAndDevTest => "With1YearASPAndDevTest"
    case With3YearASPAndDevTest => "With3YearASPAndDevTest"
    case With1YearASPAndProd    => "With1YearASPAndProd"
    case With3YearASPAndProd    => "With3YearASPAndProd"
    case With1YearRIAndDevTest  => "With1YearRIAndDevTest"
    case With3YearRIAndDevTest  => "With3YearRIAndDevTest"
    case With1YearRIAndProd     => "With1YearRIAndProd"
    case With3YearRIAndProd     => "With3YearRIAndProd"
  }
}

object PricingType {
  case object Consumption extends PricingType
  case object Reservation extends PricingType
  case object DevTestConsumption extends  PricingType


  def values: List[PricingType] = List(
    Consumption, Reservation, DevTestConsumption
  )

  def fromString(value: String): Option[PricingType] = value match {
    case "Consumption"        => Some(Consumption)
    case "Reservation"        => Some(Reservation)
    case "DevTestConsumption" => Some(DevTestConsumption)
    case _                    => None
  }

  def toString(pricingType: PricingType): String = pricingType match {
    case Consumption => "Consumption"
    case Reservation => "Reservation"
    case DevTestConsumption => "DevTestConsumption"
  }
}

object AzureSqlPaaSServiceTier extends Enumeration {
  type AzureSqlPaaSServiceTier = Value

  val GeneralPurpose: Value = Value("General Purpose")
  val BusinessCritical: Value = Value("Business Critical")
  val HyperScale: Value = Value("Hyperscale")
  val NextGenGeneralPurpose: Value = Value("Next-Gen General Purpose")
}

object ComputeTier extends Enumeration {
  type ComputeTier = Value

  val Provisioned: Value = Value(0)
  val ServerLess: Value = Value(1)
}

object AzureSqlPaaSHardwareType extends Enumeration {
  type AzureSqlPaaSHardwareType = Value

  val Gen5: Value = Value("Gen5")
  val PremiumSeries: Value = Value("Premium Series")
  val PremiumSeriesMemoryOptimized: Value = Value("Premium Series - Memory Optimized")
}

object AzureManagedDiskTier extends Enumeration {
  type AzureManagedDiskTier = Value
  val Standard, Premium, Ultra = Value
}

object MigrationAssessmentSourceTypes extends Enumeration {
  type TableTypes = Value
  val EventHubRawEventStream, Suitability, SkuRecommendationDB, SkuRecommendationMI, SkuRecommendationVM = Value
}

object MigrationAssessmentConstants{
  val DefaultLateArrivingWatermarkTime: String = "5 minutes"
  val DefaultAcrossStreamsIntervalMaxLag: String = "INTERVAL 5 MINUTES"
}


object AzureManagedDiskType extends Enumeration {
  type AzureManagedDiskType = Value
  val StandardHDD   = Value(1 << 0)  // Standard HDD
  val StandardSSD   = Value(1 << 1)  // Standard SSD
  val PremiumSSD    = Value(1 << 2)  // Premium SSD
  val UltraSSD      = Value(1 << 3)  // Ultra SSD
  val PremiumSSDV2  = Value(1 << 4)  // Premium SSD V2
}

object DiskTypeToTierMap {
  val map: Map[String, String] = Map(
    "StandardHDD"  -> "Standard",
    "StandardSSD"  -> "Standard",
    "PremiumSSD"   -> "Premium",
    "UltraSSD"     -> "Ultra",
    "PremiumSSDV2" -> "Premium"
  )
}

object ReservationTermToNumMap{
  val map: Map[String, Int] = Map(
    "1 Year"  -> 1,
    "3 Years" -> 3
  )
}


object RecommendationConstants{
  val GeneralPurpose = "General Purpose"
  val BusinessCritical = "Business Critical"
  val Hyperscale = "Hyperscale"
  val Gen5 = "Gen5"
  val PremiumSeries = "Premium Series Compute"
  val PremiumSeriesMemoryOptimized = "Premium Series Memory Optimized Compute"
  val PremiumSSDV2StorageMeterName = "Premium LRS Provisioned Capacity"
  val PremiumSSDV2ProductName = "Azure Premium SSD v2"
  val MeterUnitPerHour = "1/Hour"
  val PremiumSSDV2IOPSMeterName = "Premium LRS Provisioned IOPS"
  val PremiumSSDV2ThroughputMeterName = "Premium LRS Provisioned Throughput (MBps)"
}

