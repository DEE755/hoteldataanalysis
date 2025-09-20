import org.apache.spark.sql.DataFrame

trait StatsFunctions {
  def detectTypes(df: DataFrame): Array[(String, Set[String])]
  def getNullPercentages(df: DataFrame): DataFrame
  def getNullAboveThreshold(df: DataFrame, threshold: Double): DataFrame
} //reusability through trait
