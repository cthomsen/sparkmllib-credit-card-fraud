package de.bst.five_cities

import de.opitzconsulting.bigdata.creditcardfraud.SparkUtils
import SparkUtils.getSparkContext
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

import scala.util.Random

class GenerateTransactions extends FlatSpec with BeforeAndAfterAll {
  override def afterAll = SparkUtils.close
  private def sc = getSparkContext

  private def rInt(i: Int, o: Int = 0) = (Random nextInt i) + o
  private def rDouble(i: Int, o: Int = 0) = Math.round(Random.nextDouble * 100.0) / 100.0 * i + o
  private def rOne = scala.util.Random nextInt 2
  private val holidayC = List(
    "Deutschland",
    "Frankreich",
    "Spanien",
    "Italien",
    "Schweden",
    "Dänemark",
    "Polen",
    "Tschechien",
    "Türkei"
  )
  private val fraudC = List(
    "Deutschland",
    "Frankreich",
    "Spanien",
    "Italien",
    "Polen",
    "Polen",
    "Polen",
    "Tschechien",
    "Tschechien",
    "Türkei",
    "Bulgarien",
    "Bulgarien",
    "Rumänien",
    "Rumänien"
  )
  private def partnerId = rInt(90000) + 10000
  private val fraudPartners = partnerId :: partnerId :: partnerId :: partnerId :: Nil
  private val bookingTypes = List[Int => Seq[(Int, Int, String, String, String, Double, Int)]](
    (id: Int) => (1 until rInt(8, 1)
      map (i => (partnerId, rDouble(50, 5)))
      flatMap (b => 1 until rInt(9, 1) map (i => (id, b._1, "Online", s"01.0${10 - i}.2016", s"${rInt(14, 10)}:${rInt(50, 10)}", b._2, 0)))),
    (id: Int) => (1 until rInt(40, 1)
      map (i => (id, partnerId, "Online", s"${rInt(19, 10)}.0${rInt(9, 1)}.2016", s"${rInt(10, 10)}:${rInt(50, 10)}", rDouble(30, 10), 0))),
    (id: Int) => (1 until rInt(20, 1)
      map (i => (id, partnerId, "Online", s"0${rInt(9, 1)}.0${rInt(9, 1)}.2016", s"${rInt(10, 10)}:${rInt(50, 10)}", rDouble(300, 10), 0))),
    (id: Int) => (1 until rInt(200, 5)
      map (i => (id, partnerId, "Deutschland", s"${rInt(19, 10)}.0${rInt(9, 1)}.2016", s"${rInt(10, 10)}:${rInt(50, 10)}", rDouble(400, 20), 0))),
    (id: Int) => (1 until rInt(100, 5)
      map (i => (id, partnerId, "Deutschland", s"0${rInt(9, 1)}.0${rInt(9, 1)}.2016", s"${rInt(10, 10)}:${rInt(50, 10)}", rDouble(1000, 20), 0))),
    (id: Int) => (0 until rOne
      map (i => (rInt(9, 1), holidayC(rInt(holidayC.length))))
      flatMap (b => 1 to rInt(18, 3) map (i => (id, partnerId, b._2, s"${rInt(19, 10)}.0${b._1}.2016", s"${rInt(14, 10)}:${rInt(50, 10)}", rDouble(100, 3), 0))))
  )
  private val fraudTypes = List[Int => Seq[(Int, Int, String, String, String, Double, Int)]](
    (id: Int) =>
      (id, partnerId, fraudC(rInt(fraudC.length)), s"${rInt(19, 10)}.0${rInt(9, 1)}.2016", s"${rInt(10, 10)}:${rInt(50, 10)}", rDouble(800, 100), 1) :: Nil,
    (id: Int) => (1 to rInt(8, 1)
      map (i => (id, fraudPartners(rInt(fraudPartners.length)), "Online", s"${rInt(19, 10)}.0${rInt(9, 1)}.2016", s"${rInt(10, 10)}:${rInt(50, 10)}", rDouble(50, 5), 1))),
    (id: Int) => (1 to rInt(8, 1)
      map (i => (id, partnerId, "Deutschland", s"${rInt(19, 10)}.0${rInt(9, 1)}.2016", s"0${rInt(8)}:${rInt(50, 10)}", rDouble(400, 5), 1))),
    (id: Int) => (1 to rInt(8, 1)
      map (i => (id, fraudPartners(rInt(fraudPartners.length)), "Online", s"0${rInt(9, 1)}.0${rInt(9, 1)}.2016", s"${rInt(10, 10)}:${rInt(50, 10)}", 50.0, 1)))
  )

  "Transactions" should "be generated for training" in {
    val bookings = sc parallelize (10000 to 20000 toList) flatMap (id => if (rInt(20) == 0) fraudTypes(rInt(fraudTypes.length))(id) else Nil ::: bookingTypes flatMap (f => f(id)))
    bookings map (b => List(b._1.toString, b._2.toString, b._3, b._4, b._5, "%.2f" format b._6, b._7.toString) mkString "|") saveAsTextFile "/home/thomsen/Schreibtisch/transactions"
  }
}
