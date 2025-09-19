import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object cancellationInvestigation {

  def makeBuckets(df : DataFrame): DataFrame =
    df.withColumn("lead_time_bucket",
      when(col("lead_time") <= 7, "0-7 (last minute)")
        .when(col("lead_time") <= 30, "8-30 (short notice)")
        .when(col("lead_time") <= 90, "31-90 (medium)")
        .when(col("lead_time") <= 180, "91-180 (long)")
        .otherwise("181+ (very long)")
    )


  def cancelRatePerBuckets(dfWithBuckets: DataFrame) : DataFrame =
    dfWithBuckets.groupBy("lead_time_bucket")
      .agg(
        count("*").as("bookings"),
        avg("is_canceled").as("cancel_rate")
      )
      .orderBy("cancel_rate")


  }
