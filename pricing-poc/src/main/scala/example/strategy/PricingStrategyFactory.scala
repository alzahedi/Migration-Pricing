package example.strategy

object PricingStrategyFactory {
  def getStrategy(pricingModel: String, pricingType: String): PricingStrategy = {
    (pricingModel, pricingType) match {
      case ("PaaS", "RI")  => new ReservedPaaSPricing()
      case ("IaaS", "RI") => new ReservedIaaSPricing()
      case ("Iaas", "ASP") => new ASPIaaSPricing()
      case _ => throw new IllegalArgumentException(s"Unsupported pricing type: $pricingModel, $pricingType")
    }
  }
}
