package de.opitzconsulting.bigdata.creditcardfraud

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.regression.LinearRegression
import org.scalatest.FlatSpec

class ExplainParameters extends FlatSpec {
  "Logistic regression" should "explain parameters" in {
    println(new LogisticRegression explainParams)
  }

  "Linear regression" should "explain parameters" in {
    println(new LinearRegression explainParams)
  }
}
