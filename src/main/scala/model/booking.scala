package model

/** Minimal case class covering the columns we use.
 * Extend if you want more fields.
 */
final case class booking(
                          hotel: String,
                          isCanceled: Int,                    // 0/1
                          leadTime: Int,
                          arrivalYear: Int,
                          arrivalMonth: String,
                          arrivalDayOfMonth: Int,
                          staysWeekendNights: Int,
                          staysWeekNights: Int,
                          adults: Int,
                          children: Option[Double],           // nullable in CSV -> Option
                          babies: Int,
                          meal: String,
                          country: Option[String],
                          marketSegment: String,
                          distributionChannel: String,
                          isRepeatedGuest: Int,               // 0/1
                          previousCancellations: Int,
                          reservedRoomType: String,
                          adr: Double,                        // average daily rate
                          requiredCarParkingSpaces: Int,
                          totalOfSpecialRequests: Int
                        ) {
  val totalNights: Int = staysWeekendNights + staysWeekNights
  val totalGuests: Int = adults + babies + children.map(_.toInt).getOrElse(0)
}
