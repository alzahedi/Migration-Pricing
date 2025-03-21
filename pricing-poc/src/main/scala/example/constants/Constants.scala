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
    case "azureSqlDatabase"        => Some(AzureSqlDatabase)
    case "azureSqlManagedInstance" => Some(AzureSqlManagedInstance)
    case "azureSqlVirtualMachine"  => Some(AzureSqlVirtualMachine)
    case _                         => None
  }

  def toString(platformType: PlatformType): String = platformType match {
    case AzureSqlDatabase        => "azureSqlDatabase"
    case AzureSqlManagedInstance => "azureSqlManagedInstance"
    case AzureSqlVirtualMachine  => "azureSqlVirtualMachine"
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


  def values: List[PricingType] = List(
    Consumption, Reservation
  )

  def fromString(value: String): Option[PricingType] = value match {
    case "Consumption" => Some(Consumption)
    case "Reservation" => Some(Reservation)
    case _             => None
  }

  def toString(pricingType: PricingType): String = pricingType match {
    case Consumption => "Consumption"
    case Reservation => "Reservation"
  }
}

object AzureSqlPaaSServiceTier extends Enumeration {
  type AzureSqlPaaSServiceTier = Value

  val GeneralPurpose: Value = Value("GeneralPurpose")
  val BusinessCritical: Value = Value("BusinessCritical")
  val HyperScale: Value = Value("Hyperscale")
  val NextGenGeneralPurpose: Value = Value("NextGenGeneralPurpose")
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

