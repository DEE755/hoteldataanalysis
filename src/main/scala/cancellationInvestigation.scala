import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object cancellationInvestigation {

  def investigate(df: DataFrame): Unit = {
    println("Cancellation Investigations\n We would like to understand which are the main factors that lead to a booking cancellation.\n")


  println("We can start by looking at the time between the date a reservation is made and the date the reservation is for :\n" +
    "this is on our dataset the column lead_time and it is expressed in days.\n")

  df.select("lead_time").describe().show()

    println("We want to investigate further the lead_time column, so we can look at its distribution:\n" +
    "we will create bins of several days and count the number of bookings in each bin.\n")

    val withBuckets = df.withColumn("lead_time_bucket",
      when(col("lead_time") <= 7, "0-7 (last minute)")
        .when(col("lead_time") <= 30, "8-30 (short notice)")
        .when(col("lead_time") <= 90, "31-90 (medium)")
        .when(col("lead_time") <= 180, "91-180 (long)")
        .otherwise("181+ (very long)")
    )
  println("Then we can count the number of bookings in each bin and show the cancel rate of each bin and rank them by the ones that have the most cancelation rate\n")

    val cancelByBucket = withBuckets.groupBy("lead_time_bucket")
      .agg(
        count("*").as("bookings"),
        avg("is_canceled").as("cancel_rate")
      )
      .orderBy("cancel_rate")

    cancelByBucket.show()

    //df.select("lead_time").groupBy("lead_time").count().orderBy("lead_time").show(100)

}

  }
