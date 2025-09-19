

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import statisticsFunctions._
import cancellationInvestigation._
import ParsingUtils._

import scala.Console.println

  object SparkApp {
    def main(args: Array[String]): Unit = {
      implicit val spark = SparkSession.builder()
        .appName("Hotel Analysis")
        .master("local[*]")
        .getOrCreate()

      //LESS VERBOSE LOGS
      spark.sparkContext.setLogLevel("FATAL")


      val df = spark.read
        .option("header", "true")
        .option("inferSchema", "true") //infer schema to have the correct types
        .csv("data/hotel_booking.csv")
      //df.show(10)


      println("\n\n//////////////////////////////////////////////INTRO///////////////////////////////////////\n\n")
      //df.printSchema()
      //df.describe().show()


      /*println("those are the detected types in the dataset:")
   statisticsFunctions.detectTypes(df)
      .flatMap(_._2)
      .distinct
      .foreach(println)

      println ("let's check if one or more of the columns have more than one type")

      val columnsWithMultipleTypes = statisticsFunctions.detectTypes(df).filter(_._2.size > 1)
      if (columnsWithMultipleTypes.isEmpty) {
        println("No columns with multiple types detected.\n")
      } else {
        println("Columns with multiple types detected:")
        columnsWithMultipleTypes.foreach { case (colName, types) =>
          println(s"Column: $colName, Types: ${types.mkString(", ")}")
        }
      }*/



println("\nNow let's calculate the percentage of null values in each column:")
      statisticsFunctions.getNullPercentages(df).show()

      println("\nLet's detect if there are column with more than 10% of missing values:")

      val columnsAboveThreshold = statisticsFunctions.getNullAboveThreshold(df, 10.0)
      println( s"${columnsAboveThreshold.count()} columns found having more than 10% missing\n")

      println("Let's check which they are and their percentage of null values:")
      statisticsFunctions.getNullPercentages(columnsAboveThreshold).show()

      println("We can decide to drop those columns if we want:")
      val dfCleaned = df.drop(columnsAboveThreshold.columns:_*)
      println(s"Now the dataset has ${dfCleaned.columns.length} columns instead of ${df.columns.length}\n")




      println("\n////////A.CANCELLATION INVESTIGATION///////\n")

      println("Cancellation Investigations\n We would like to understand which are the main factors that lead to a booking cancellation.\n")


      println("\n//1.LEAD TIME INVESTIGATION\n")

      println("We can start by looking at the time between the date a reservation is made and the date the reservation is for :\n" +
        "this is on our dataset the column lead_time and it is expressed in days.\n")

      df.select("lead_time").describe().show()


      println("We want to investigate further the lead_time column, so we can look at its distribution:\n" +
        "we will create bins of several days and count the number of bookings in each bin.\n")

        val dfWithBuckets=cancellationInvestigation.lead_time.makeBuckets(dfCleaned)


      println("We can now look at the cancellation rate per bucket:\n" + "we will also save the result in a csv file.\n")
      val cancelRatePerBucket=cancellationInvestigation.lead_time.cancelRatePerBuckets(dfWithBuckets)

      //showing the result
      cancelRatePerBucket.show()
      // Saving the result as a csv file
      cancelRatePerBucket
        .coalesce(1)//make it onluy one file
        .write
        .mode("overwrite")//if it already exists overwrite
        .option("header", "true")//add header
        .csv("output/cancel_by_lead")//path to save the file



      println("\nTotal Nights investigation\n")

      //Making a new column total_nights:
      println("We will look at the total number of nights per booking, which is the sum of weekend and week nights.\n")
      dfCleaned.select("stays_in_weekend_nights", "stays_in_week_nights").describe().show()
      val dfWithTotalNights = cancellationInvestigation.total_nights.makeTotalNights(dfCleaned)
      dfWithTotalNights.select("total_nights").describe().show()

      //CREATING A DATASET[BOOKING] WITH ONLY THE COLUMNS WE NEED AND VALIDATING THE ROWS
      val ParseResult(validBookings, invalidRows) = toValidatedBookings(df)

      // optional audit output
      println(s"Invalid rows: ${invalidRows.count()}")
      invalidRows.coalesce(1).write.mode("overwrite").text("output/audit_invalid_rows")





      //filtering bookings with lead time >= 90 days and total nights >= 4
     val dsFiltered = cancellationInvestigation.total_nights.filterLongLeadsAndStays(validBookings, 90, 4)

      println("We filtered the dataset to keep only bookings with lead time >= 90 days and total nights >= 4:\n")
      dsFiltered.show(15)
      println("Now we can look at the cancellation rate for bookings with lead time >= 90 days and total nights >= 4:\n")

      cancellationInvestigation.total_nights.cancelRateForLongLeadsAndStays(dsFiltered).show()




















      println("\n\n//////////////////////////////////////////////END///////////////////////////////////////")
      spark.stop()
    }
  }


