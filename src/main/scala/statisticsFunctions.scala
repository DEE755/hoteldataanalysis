import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, when, lit}


object statisticsFunctions {


  def detectTypes(df: DataFrame): Array[(String, Set[String])] = {
      df.columns.map { c =>
        val values = df.select(col(c))
          .distinct()
          .na.drop()
          .limit(1000)
          .collect()
          .map(_.getString(0))
          .toSet

        val types =
          if (values.nonEmpty && values.forall(v => v == "0" || v == "1")) Set("Boolean")
          else if (values.forall(v => v.matches("^-?[0-9]+$"))) Set("Integer")
          else if (values.forall(v => v.matches("^-?[0-9]*\\.[0-9]+$"))) Set("Double")
          else Set("String")

        (c, types)
      }
    }

  def getNullPercentages(df: DataFrame): DataFrame = {
    val totalRows = df.count().toDouble
    df.select(df.columns.map { c =>
      ((count(when(col(c).isNull, 1)).cast("double") / lit(totalRows)) * 100).alias(c)
    }: _*)
  }



  def getNullAboveThreshold(df: DataFrame, threshold: Double): DataFrame = {

    val columnsAboveThreshold = df.columns.filter { c =>
      val nullPercentage =  getNullPercentages(df).select(col(c)).first().getDouble(0)
      nullPercentage > threshold
    }
    df.select(columnsAboveThreshold.map(col): _*)
  }




}
