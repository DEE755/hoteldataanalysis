

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import StatisticsFunctions._

trait SparkSessionTestWrapper {
  lazy val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("Spark Test")
    .getOrCreate()
}


class statisticsFunctionsTest extends AnyFunSuite with SparkSessionTestWrapper {

  test("detectTypes identifies Boolean columns") {
    val df = spark.createDataFrame(Seq(
      ("0", "1"),
      ("1", "0"),
      ("0", "1")
    )).toDF("col1", "col2")

    val result = StatisticsFunctions.detectTypes(df)
    assert(result.contains("col1" -> Set("Boolean")))
    assert(result.contains("col2" -> Set("Boolean")))
  }

  test("detectTypes identifies Integer columns") {
    val df = spark.createDataFrame(Seq(
      ("123", "456"),
      ("789", "0"),
      ("-1", "42")
    )).toDF("col1", "col2")

    val result = StatisticsFunctions.detectTypes(df)
    assert(result.contains("col1" -> Set("Integer")))
    assert(result.contains("col2" -> Set("Integer")))
  }

  test("detectTypes identifies Double columns") {
    val df = spark.createDataFrame(Seq(
      ("1.23", "4.56"),
      ("7.89", "0.0"),
      ("-1.1", "42.42")
    )).toDF("col1", "col2")

    val result = StatisticsFunctions.detectTypes(df)
    assert(result.contains("col1" -> Set("Double")))
    assert(result.contains("col2" -> Set("Double")))
  }


  test("getNullPercentages calculates correct percentages") {
    val df = spark.createDataFrame(Seq(
      (null, "value1"),
      ("value2", null),
      (null, null)
    )).toDF("col1", "col2")

    val result = StatisticsFunctions.getNullPercentages(df).collect()
    assert(result(0).getDouble(0) == 66.66666666666666) // col1
    assert(result(0).getDouble(1) == 66.66666666666666) // col2
  }

  test("getNullAboveThreshold filters columns correctly") {
    val df = spark.createDataFrame(Seq(
      (null, "value1"),
      ("value2", null),
      (null, null)
    )).toDF("col1", "col2")

    val result = StatisticsFunctions.getNullAboveThreshold(df, 50.0)
    assert(result.columns.contains("col1"))
    assert(result.columns.contains("col2"))
  }
}

