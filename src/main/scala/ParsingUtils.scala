import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import scala.util.Try

/**
 * Utility object for parsing and validating booking data from a DataFrame.
 */
object ParsingUtils {

  /**
   * Case class representing the result of parsing: valid bookings and invalid rows.
   *
   * @param valid   Dataset of valid `Booking` objects.
   * @param invalid Dataset of invalid rows as error messages.
   */
  final case class ParseResult(valid: Dataset[Booking], invalid: Dataset[String])

  /**
   * Case class representing a booking with essential fields.
   *
   * @param is_canceled  Indicates if the booking was canceled (1 for canceled, 0 otherwise).
   * @param lead_time    Number of days between booking and arrival.
   * @param total_nights Total number of nights for the booking.
   */
  final case class Booking(is_canceled: Int, lead_time: Int, total_nights: Int)

  /**
   * Converts a string to an integer, returning an `Either` with an error message or the parsed integer.
   *
   * @param s     The string to parse.
   * @param field The name of the field being parsed (used in error messages).
   * @return      `Right` with the parsed integer, or `Left` with an error message.
   */
  def toIntE(s: String, field: String): Either[String, Int] =
    Option(s).toRight(s"$field: null").flatMap(v =>
      scala.util.Try(v.trim.toInt).toEither.left.map(_ => s"$field: invalid int '$v'")
    )

  /** Type alias for a list of error messages. */
  type Errs = List[String]

  /** Type alias for a validation result, represented as an `Either` of errors or a valid value. */
  type V[A] = Either[Errs, A]

  /**
   * Combines two validation results into one, applying a function if both are valid.
   *
   * @param va First validation result.
   * @param vb Second validation result.
   * @param f  Function to combine the valid values.
   * @return   Combined validation result.
   */
  def map2[A, B, C](va: V[A], vb: V[B])(f: (A, B) => C): V[C] =
    (va, vb) match {
      case (Right(a), Right(b)) => Right(f(a, b))
      case (Left(e1), Right(_)) => Left(e1)
      case (Right(_), Left(e2)) => Left(e2)
      case (Left(e1), Left(e2)) => Left(e1 ++ e2)
    }

  /**
   * Combines three validation results into one, applying a function if all are valid.
   *
   * @param va First validation result.
   * @param vb Second validation result.
   * @param vc Third validation result.
   * @param f  Function to combine the valid values.
   * @return   Combined validation result.
   */
  def map3[A, B, C, D](va: V[A], vb: V[B], vc: V[C])(f: (A, B, C) => D): V[D] =
    map2(map2(va, vb)((a, b) => (a, b)), vc) { case ((a, b), c) => f(a, b, c) }

  /**
   * Parses a row of booking data into a `Booking` object, validating each field.
   *
   * @param isCanceled String representation of the `is_canceled` field.
   * @param leadTime   String representation of the `lead_time` field.
   * @param wend       String representation of the `stays_in_weekend_nights` field.
   * @param week       String representation of the `stays_in_week_nights` field.
   * @return           Validation result: `Right` with a `Booking` object, or `Left` with errors.
   */
  def parseBooking(isCanceled: String, leadTime: String, wend: String, week: String): V[Booking] = {
    val cE = toIntE(isCanceled, "is_canceled").left.map(List(_))
    val lE = toIntE(leadTime, "lead_time").left.map(List(_))
    val weE = toIntE(wend, "stays_in_weekend_nights").left.map(List(_))
    val wkE = toIntE(week, "stays_in_week_nights").left.map(List(_))
    val nightsE = map2(weE, wkE)(_ + _)
    map3(cE, lE, nightsE) { (c, l, n) => Booking(c, l, n) }
  }

  /**
   * Converts a DataFrame into a `ParseResult` containing valid bookings and invalid rows.
   *
   * @param df    Input DataFrame with booking data.
   * @param spark Implicit SparkSession for Dataset operations.
   * @return      `ParseResult` containing valid bookings and invalid rows.
   */
  def toValidatedBookings(df: DataFrame)(implicit spark: SparkSession): ParseResult = {
    import spark.implicits._

    // Select and cast relevant columns from the DataFrame
    val raw = df.select(
      col("is_canceled").cast("string"),
      col("lead_time").cast("string"),
      col("stays_in_weekend_nights").cast("string"),
      col("stays_in_week_nights").cast("string")
    ).toDF("is_canceled", "lead_time", "wend", "week")

    // Parse rows into Either[Errs, Booking]
    val parsed: Dataset[Either[Errs, Booking]] =
      raw.as[(String, String, String, String)].map { case (c, l, we, wk) => parseBooking(c, l, we, wk) }

    // Separate valid bookings and invalid rows
    val valids = parsed.flatMap { case Right(b) => Some(b); case _ => None }
    val invalid = parsed.flatMap { case Left(errs) => Some(errs.mkString("; ")); case _ => None }

    ParseResult(valids, invalid)
  }
}