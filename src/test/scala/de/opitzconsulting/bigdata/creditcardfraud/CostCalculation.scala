package de.opitzconsulting.bigdata.creditcardfraud

import org.scalatest.FlatSpec

class CostCalculation extends FlatSpec {
  val dataSource = getClass getResource "/transactions"

  "Average costs pre fraud" should "be calculated" in {
    val frauds = SparkUtils.getSparkContext textFile dataSource.getPath map (_ split "\\|") filter (_(6) == "1")
    frauds.cache
    val fraudCosts = frauds map (_(5).replace(",", ".").toFloat) reduce (_ + _)
    println(fraudCosts / frauds.count)
  }

  "Average profit per transaction" should "be calculated" in {
    val transactions = SparkUtils.getSparkContext textFile dataSource.getPath map (_ split "\\|")
    transactions.cache
    val transactionProfits = transactions map (_(5).replace(",", ".").toFloat) reduce (_ + _)
    println(0.02 * transactionProfits / transactions.count)
  }
}
