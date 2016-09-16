package de.opitzconsulting.bigdata.creditcardfraud

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkUtils {
  private var sparkContext: Option[SparkContext] = None
  private var sparkSQLContext: Option[SQLContext] = None

  def getSparkContext = sparkContext getOrElse {
    val sparkConf: SparkConf = new SparkConf
    sparkConf.setAppName("Hallo Spark!")
    sparkConf.setMaster("local[*]")
    sparkContext = Some(new SparkContext(sparkConf))
    sparkContext.get
  }

  def getSparkSQLContext = sparkSQLContext getOrElse {
    sparkSQLContext = Some(new SQLContext(getSparkContext))
    sparkSQLContext.get
  }

  def close {
    sparkContext foreach (_.stop)
    sparkContext = None
  }

  def debugPrint[T](something: T): T = {
    println(s">> $something")
    something
  }
}
