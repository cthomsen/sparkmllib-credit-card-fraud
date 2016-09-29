package de.opitzconsulting.bigdata.creditcardfraud

import java.sql.Date
import java.text.SimpleDateFormat
import java.util.{Calendar, GregorianCalendar}

import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.{NaiveBayesModel, NaiveBayes, LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.regression.{LinearRegressionModel, LinearRegression}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{RandomForest, DecisionTree}
import org.apache.spark.mllib.tree.model.{RandomForestModel, DecisionTreeModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

object ModelTraining {
  def main (args: Array[String]) {
    val data = prepareData(SparkUtils.getSparkContext, SparkUtils.getSparkSQLContext)
    val Array(training, test) = data randomSplit (Array(0.7, 0.3))
    training.cache
    test.cache
    val predictionNullHypothesis = test select ("label") map (r => 0.0 -> r(0).toString.toDouble)
    val modelLogisticRegression = trainLogisticRegression(training)
    val predictionLogisticRegression = testLogisticRegression(modelLogisticRegression, test)
    val modelLinearRegression = trainLinearRegression(training)
    val predictionLinearRegression = testLinearRegression(modelLinearRegression, test)
    val modelDecisonTree = trainDecisonTree(training)
    val predictionDecisionTree = testDecisonTree(modelDecisonTree, test)
    val modelRandomForest = trainRandomForest(training)
    val predictionRandomForest = testRandomForest(modelRandomForest, test)
    val modelNaiveBayes = trainNaiveBayes(training)
    val predictionNaiveBayes = testNaiveBayes(modelNaiveBayes, test)
    val modelSVM = trainSVM(training)
    val predictionSVM = testSVM(modelSVM, test)
    printResults(predictionNullHypothesis, "Null Hypothese")
    printResults(predictionLogisticRegression, "Logistische Regression")
    printResults(predictionLinearRegression, "Lineare Regression")
    printResults(predictionDecisionTree, "Decision Tree")
    printResults(predictionRandomForest, "Random Forest")
    printResults(predictionNaiveBayes, "Naive Bayes")
    printResults(predictionSVM, "SVM")
    SparkUtils.close
  }

  def prepareData(sc: SparkContext, sql: SQLContext) = {
    val dataSource = getClass getResource "/transactions"
    val transactions = sc textFile dataSource.getPath map {
      r =>
        val dateFormat = new SimpleDateFormat("dd.MM.yyyy HH:mm")
        val a = r split "\\|"
        val date = new Date(dateFormat parse s"${a(3)} ${a(4)}" getTime)
        Transaction(
          a(0),
          a(1),
          a(2),
          date,
          a(5).replace(",", ".").toFloat,
          a(6) == "1"
        )
    }
    import sql.implicits._
    transactions map {
      t =>
        val cal = new GregorianCalendar
        cal setTime t.date
        (if (t.fraud) 1.0 else 0.0) -> (Vectors dense (
          t.amount.toDouble, // amount of transaction
          t.partnerId.toDouble,
          if (t.location == "Online") 1.0 else 0.0, // is online
          if (t.location == "Deutschland") 0.0 else if (t.location == "Online") 2.0 else 1.0, // is abroad
          cal get Calendar.HOUR_OF_DAY toDouble, // hour of day
          cal get Calendar.DAY_OF_MONTH toDouble)) // day of month
    } toDF ("label", "features")
  }

  def trainLogisticRegression(training: DataFrame) = {
    val lr = new LogisticRegression
    lr.setThreshold(0.023)
    lr fit training
  }

  def testLogisticRegression(model: LogisticRegressionModel, test: DataFrame) =
    model transform test select ("prediction", "label") map (r => r(0).toString.toDouble -> r(1).toString.toDouble)

  def trainLinearRegression(training: DataFrame) = {
    val lr = new LinearRegression
    lr fit training
  }

  def testLinearRegression(model: LinearRegressionModel, test: DataFrame) =
    (model transform test select("prediction", "label")
      map (r => (if (r(0).toString.toDouble < 0.0058) 0.0 else 1.0) -> r(1).toString.toDouble))

  def trainDecisonTree(training: DataFrame) =
    DecisionTree trainClassifier (toVectorRDD(training), 2, Map[Int, Int](), "gini", 10, 32)

  def testDecisonTree(model: DecisionTreeModel, test: DataFrame) =
    toVectorRDD(test) map (r => (model predict r.features, r.label))

  def trainRandomForest(training: DataFrame) =
    RandomForest trainClassifier (toVectorRDD(training), 2, Map[Int, Int](), 50, "auto", "gini", 10, 32)

  def testRandomForest(model: RandomForestModel, test: DataFrame) =
    toVectorRDD(test) map (r => (model predict r.features, r.label))

  def trainNaiveBayes(training: DataFrame) = {
    val nb = new NaiveBayes
    nb setSmoothing 3.0
    nb fit training
  }

  def testNaiveBayes(model: NaiveBayesModel, test: DataFrame) =
    model transform test select ("prediction", "label") map (r => r(0).toString.toDouble -> r(1).toString.toDouble)

  def trainSVM(training: DataFrame) = {
    val training2 = toVectorRDD(training)
    training2.cache
    SVMWithSGD train (training2, 1000, 0.1, 0.5)
  }

  def testSVM(model: SVMModel, test: DataFrame) =
    toVectorRDD(test) map (r => (model predict r.features, r.label))

  def printResults(prediction: RDD[(Double, Double)], modelName: String) = {
    val truePositives = (prediction filter (r => r._1 == 1.0 && r._2 == 1.0) count)
    val falsePositives = (prediction filter (r => r._1 == 1.0 && r._2 == 0.0) count)
    val trueNegatives = (prediction filter (r => r._1 == 0.0 && r._2 == 0.0) count)
    val falseNegatives = (prediction filter (r => r._1 == 0.0 && r._2 == 1.0) count)
    println(s"\n${modelName.toUpperCase}")
    println(s"True positives: $truePositives")
    println(s"False positives: $falsePositives")
    println(s"True negatives: $trueNegatives")
    println(s"False negatives: $falseNegatives")
    println(s"Kosten: ${"%.2f" format (falsePositives * 5.16 + falseNegatives * 125.08)} €")
  }

  def toVectorRDD(dataFrame: DataFrame) =
    dataFrame select ("label", "features") map (r => new LabeledPoint(r(0).toString.toDouble, r(1).asInstanceOf[Vector]))
}
