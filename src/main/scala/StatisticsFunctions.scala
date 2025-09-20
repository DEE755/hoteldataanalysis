import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, when, lit}



object StatisticsFunctions extends StatsFunctions {


  /**
   * Detects the data types of columns in a DataFrame based on their values.
   *
   * @param df The input DataFrame.
   * @return   An array of tuples where each tuple contains the column name and a set of detected types.
   *           Possible types are "Boolean", "Integer", "Double", and "String".
   */
  def detectTypes(df: DataFrame): Array[(String, Set[String])] = {
      df.columns.map { c =>
        val values = df.select(col(c))
          .distinct()
          .na.drop()
          .limit(1000)
          .collect()
          .map(_.get(0).toString)
          .toSet

        val types =
          if (values.nonEmpty && values.forall(v => v == "0" || v == "1")) Set("Boolean")
          else if (values.forall(v => v.matches("^-?[0-9]+$"))) Set("Integer")
          else if (values.forall(v => v.matches("^-?[0-9]*\\.[0-9]+$"))) Set("Double")
          else Set("String")

        (c, types)
      }
    }

  /**
   * Calculates the percentage of null values for each column in a DataFrame.
   *
   * @param df The input DataFrame.
   * @return   A DataFrame where each column represents the percentage of null values for the corresponding column in the input DataFrame.
   */
  def getNullPercentages(df: DataFrame): DataFrame = {
    val totalRows = df.count().toDouble
    df.select(df.columns.map { c =>
      ((count(when(col(c).isNull, 1)).cast("double") / lit(totalRows)) * 100).alias(c)
    }: _*)
  }

  /**
   * Filters columns in a DataFrame based on a null value percentage threshold.
   *
   * @param df        The input DataFrame.
   * @param threshold The threshold percentage for null values. Columns with a null percentage above this value will be included.
   * @return          A DataFrame containing only the columns with null percentages above the specified threshold.
   */
  def getNullAboveThreshold(df: DataFrame, threshold: Double): DataFrame = {

    val columnsAboveThreshold = df.columns.filter { c =>
      val nullPercentage =  getNullPercentages(df).select(col(c)).first().getDouble(0)
      nullPercentage > threshold
    }
    df.select(columnsAboveThreshold.map(col): _*)
  }

}