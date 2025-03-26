package example.strategy

object PricingStrategyFactory {
  def getStrategy(
      pricingModel: String,
      pricingType: String,
      environment: String
  ): PricingStrategy = {
    (pricingModel, pricingType, environment) match {
      case ("PaaS", "RI", "Prod")  => new ReservedProdPaaSPricing()
      case ("IaaS", "RI", "Prod")  => new ReservedProdIaaSPricing()
      case ("IaaS", "ASP", "Prod") => new ASPProdIaaSPricing()
      case ("IaaS", "ASP", "DevTest") => new ASPDevTestIaaSPricing()
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported pricing type: $pricingModel, $pricingType"
        )
    }
  }
}
