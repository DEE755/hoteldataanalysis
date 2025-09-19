import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.Encoders
    import ParsingUtils._

    /**
     * Object for investigating booking cancellations using Spark DataFrames and Datasets.
     */
    object cancellationInvestigation {

      /**
       * Object for operations related to lead time analysis.
       */
      object lead_time {

        /**
         * Adds a "lead_time_bucket" column to the DataFrame, categorizing lead times into buckets.
         *
         * @param df Input DataFrame containing a "lead_time" column.
         * @return   DataFrame with an additional "lead_time_bucket" column.
         */
        def makeBuckets(df: DataFrame): DataFrame =
          df.withColumn("lead_time_bucket",
            when(col("lead_time") <= 7, "0-7 (last minute)")
              .when(col("lead_time") <= 30, "8-30 (short notice)")
              .when(col("lead_time") <= 90, "31-90 (medium)")
              .when(col("lead_time") <= 180, "91-180 (long)")
              .otherwise("181+ (very long)")
          )

        /**
         * Calculates the cancellation rate per lead time bucket.
         *
         * @param dfWithBuckets DataFrame with a "lead_time_bucket" column.
         * @return              DataFrame with the number of bookings and cancellation rate per bucket.
         */
        def cancelRatePerBuckets(dfWithBuckets: DataFrame): DataFrame =
          dfWithBuckets.groupBy("lead_time_bucket")
            .agg(
              count("*").as("bookings"),
              avg("is_canceled").as("cancel_rate")
            )
            .orderBy("cancel_rate")
      }

      /**
       * Object for operations related to total nights analysis.
       */
      object total_nights {

        /**
         * Adds a "total_nights" column to the DataFrame, summing weekend and weekday stays.
         *
         * @param df Input DataFrame with "stays_in_weekend_nights" and "stays_in_week_nights" columns.
         * @return   DataFrame with an additional "total_nights" column.
         */
        def makeTotalNights(df: DataFrame): DataFrame =
          df.withColumn("total_nights", col("stays_in_weekend_nights") + col("stays_in_week_nights"))

        // Implicit encoder for the Booking case class
        implicit val bookingEncoder = Encoders.product[Booking]

        /**
         * Object for parsing booking data into the Booking case class.
         */
        object BookingParser {

          /**
           * Parses booking data from strings into a validated Booking object.
           *
           * @param isCanceled String representation of the "is_canceled" field.
           * @param leadTime   String representation of the "lead_time" field.
           * @param wend       String representation of the "stays_in_weekend_nights" field.
           * @param week       String representation of the "stays_in_week_nights" field.
           * @return           Validation result: `Right` with a Booking object, or `Left` with errors.
           */
          def parseBooking(isCanceled: String, leadTime: String, wend: String, week: String): V[Booking] = {
            val cE = toIntE(isCanceled, "is_canceled").left.map(List(_))
            val lE = toIntE(leadTime, "lead_time").left.map(List(_))
            val weE = toIntE(wend, "stays_in_weekend_nights").left.map(List(_))
            val wkE = toIntE(week, "stays_in_week_nights").left.map(List(_))

            val nightsE = map2(weE, wkE)(_ + _)
            map3(cE, lE, nightsE){ (c, l, n) => Booking(c, l, n) }
          }
        }

        /**
         * Converts a DataFrame into a Dataset of Booking objects.
         *
         * @param df Input DataFrame with booking data.
         * @return   Dataset of Booking objects.
         */
        def toBookingDS(df: DataFrame): Dataset[Booking] =
          df.select(
            col("is_canceled").cast("int"), col("lead_time").cast("int"),
            col("adr").cast("double"),
            (col("stays_in_weekend_nights").cast("int") + col("stays_in_week_nights").cast("int")).as("total_nights")
          ).as[Booking]

        /**
         * Predicate to check if a booking has a lead time greater than or equal to a given value.
         *
         * @param n Minimum lead time.
         * @return  Predicate function for filtering bookings.
         */
        def longLead(n: Int)(b: Booking): Boolean = b.lead_time >= n

        /**
         * Predicate to check if a booking has a total stay greater than or equal to a given value.
         *
         * @param n Minimum total nights.
         * @return  Predicate function for filtering bookings.
         */
        def longStay(n: Int)(b: Booking): Boolean = b.total_nights >= n

        /**
         * Combines two predicates into one using logical AND.
         *
         * @param p First predicate.
         * @param q Second predicate.
         * @return  Combined predicate.
         */
        def andP[A](p: A => Boolean, q: A => Boolean): A => Boolean = a => p(a) && q(a)

        /**
         * Filters a Dataset of Booking objects based on minimum lead time and total nights.
         *
         * @param ds       Dataset of Booking objects.
         * @param minLead  Minimum lead time.
         * @param minStay  Minimum total nights.
         * @return         Filtered Dataset of Booking objects.
         */
        def filterLongLeadsAndStays(ds: Dataset[Booking], minLead: Int, minStay: Int): Dataset[Booking] = {
          val keep = andP(longLead(minLead), longStay(minStay)) // combine predicates to filter
          ds.filter(keep) // apply filter to Dataset and return result
        }

        /**
         * Calculates the cancellation rate for bookings with long lead times and stays.
         *
         * @param dsFiltered Filtered Dataset of Booking objects.
         * @return           DataFrame with the average cancellation rate.
         */
        def cancelRateForLongLeadsAndStays(dsFiltered: Dataset[Booking]): DataFrame =
          dsFiltered.groupBy().agg(avg("is_canceled").as("cancel_rate"))
      }
    }