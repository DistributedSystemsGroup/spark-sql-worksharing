package util

object Tables {
  def getOneTable() = Seq("customer")

  def getSomeTables() = Seq("date_dim", "store_sales", "item")

  def getAllTables() = Seq("catalog_sales", "catalog_returns",
      "inventory", "store_sales", "store_returns", "web_sales", "web_returns",
      "call_center", "catalog_page", "customer", "customer_address", "customer_demographics",
      "date_dim", "household_demographics", "income_band", "item", "promotion", "reason",
      "ship_mode", "store", "time_dim", "warehouse", "web_page", "web_site")
}
