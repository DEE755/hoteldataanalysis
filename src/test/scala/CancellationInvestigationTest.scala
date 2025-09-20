import ParsingUtils.Booking
import CancellationInvestigation.total_nights.{andP, longLead, longStay}
import org.scalatest.funsuite.AnyFunSuite

class CancellationInvestigationTest extends AnyFunSuite with SparkSessionTestWrapper
{

  // Predicate tests
  test("longLead predicate") {
    val b = Booking(0, 100, 2)
    assert(longLead(90)(b))
    assert(!longLead(101)(b))
  }
  test("longStay predicate") {
    val b = Booking(0, 10, 5)
    assert(longStay(4)(b))
    assert(!longStay(6)(b))
  }
  test("andP combines predicates") {
    val b = Booking(0, 100, 5)
    val pred = andP(longLead(90), longStay(5))
    assert(pred(b))
    assert(!andP(longLead(101), longStay(5))(b))
  }

}
