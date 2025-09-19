

import org.apache.spark.sql.DataFrame

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._
  import statisticsFunctions._
  import cancellationInvestigation._

  object SparkApp {
    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder()
        .appName("SimpleSparkApp")
        .master("local[*]")
        .getOrCreate()

      //LESS VERBOSE LOGS
      spark.sparkContext.setLogLevel("FATAL")

      import spark.implicits._

      val df = spark.read
        .option("header", "true")
        .option("inferSchema", "true") //infer schema to have the correct types
        .csv("data/hotel_booking.csv")
      //df.show(10)


      println("\n\n//////////////////////////////////////////////START///////////////////////////////////////\n\n")
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



      cancellationInvestigation.investigate(dfCleaned)











      println("\n\n//////////////////////////////////////////////END///////////////////////////////////////")
      spark.stop()
    }
  }


