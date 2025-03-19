package example.constants

sealed trait PlatformType

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