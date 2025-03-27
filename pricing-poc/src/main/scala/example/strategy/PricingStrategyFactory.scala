package example.strategy

object PricingStrategyFactory {
  def getStrategy(
      pricingModel: String,
      pricingType: String,
      environment: String,
      reservationTerm: String
  ): PricingStrategy = {
    (pricingModel, pricingType, environment) match {
      case ("PaaS", "RI", "Prod")  => new ReservedProdPaaSPricing(reservationTerm)
      case ("IaaS", "RI", "Prod")  => new ReservedProdIaaSPricing(reservationTerm)
      case ("IaaS", "ASP", "Prod") => new ASPProdIaaSPricing(reservationTerm)
      case ("IaaS", "ASP", "DevTest") => new ASPDevTestIaaSPricing(reservationTerm)
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported pricing type: $pricingModel, $pricingType"
        )
    }
  }
}
