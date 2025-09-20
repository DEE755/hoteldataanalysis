import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import ParsingUtils._
import CancellationInvestigation.lead_time.{cancelRatePerBuckets, makeBuckets}
import StatisticsFunctions._
import CancellationInvestigation._
import CancellationInvestigation.total_nights.{andP, longLead, longStay}


class ParsingUtilsSpec extends AnyFunSuite with SparkSessionTestWrapper {

  // Pure function tests
  test("toIntE parses valid int") {
    assert(toIntE("42", "test") == Right(42))
  }
  test("toIntE fails on invalid int") {
    assert(toIntE("abc", "test").left.exists(_.contains("invalid int")))
  }

  test("parseBooking returns Right for valid row") {
    val result = parseBooking("0", "10", "2", "3")
    assert(result == Right(Booking(0, 10, 5)))
  }
  test("parseBooking returns Left for invalid row") {
    val result = parseBooking("x", "y", "z", "w")
    assert(result.isLeft)
  }

  // Combinator tests
  test("map2 combines two Rights") {
    val r = map2(Right(1), Right(2))(_ + _)
    assert(r == Right(3))
  }
  test("map2 combines Right and Left") {
    val r = map2(Right(1), Left(List("err")))((_, _) => 0)
    assert(r == Left(List("err")))
  }
  test("map2 combines two Lefts") {
    val r = map2(Left(List("e1")), Left(List("e2")))((_, _) => 0)
    assert(r == Left(List("e1", "e2")))
  }


}