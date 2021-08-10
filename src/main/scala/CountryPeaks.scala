import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.functions.{broadcast, col, date_add, expr, udf, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scalafx.application.JFXApp.PrimaryStage
import scalafx.scene.Scene
import scalafx.scene.chart.{CategoryAxis, LineChart, NumberAxis, XYChart}
import scalafx.collections.ObservableBuffer

object CountryPeaks {
  def countryWeekly(name:String, spark:SparkSession): DataFrame = {
    try {
      val weeklyTotal = spark.sql(s"Select * from covid_19 where Country_Region = '$name' " +
        s"and date_format(ObservationDate, 'u') = 6 order by ObservationDate")
        .toDF("Country_Region", "Date", "Confirmed", "Deaths", "Recovered")

      val broadcastWeekly = broadcast(weeklyTotal
        .withColumn("prevWeek", date_add(col("Date"), 7))
        .select(col("prevWeek"),
          col("Confirmed").as("prev_Confirmed"),
          col("Deaths").as("prev_Deaths"),
          col("Recovered").as("prev_Recovered")))

      spark.conf.set("spark.sql.crossJoin.enabled", "true")
      val joinedWeekly = weeklyTotal.join(broadcastWeekly,
        weeklyTotal("Date") === broadcastWeekly("prevWeek"), "left")

      def diff = udf((num: Long, prev_num: Long) => num - prev_num)

      joinedWeekly
        .withColumn("new_cases", when(col("prev_Confirmed").isNull, col("Confirmed")).otherwise(diff(col("Confirmed"), col("prev_Confirmed"))))
        .withColumn("new_deaths", when(col("prev_deaths").isNull, col("Deaths")).otherwise(diff(col("Deaths"), col("prev_Deaths"))))
        .withColumn("new_recovered", when(col("prev_Recovered").isNull, col("Recovered")).otherwise(diff(col("Recovered"), col("prev_recovered"))))
        .select("Date", "new_cases", "new_deaths", "new_recovered")
    }
    catch {
      case ex: ParseException => null
    }
  }

  def apply(country: String, spark:SparkSession): DataFrame = countryWeekly(country, spark)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("hello hive")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("Use project2")
    val countries = spark.sql("Select distinct Country_Region from covid_19").collect()
//    val startTime = System.nanoTime()
    val country = "US"
    val dataDF = countryWeekly(country, spark)
//    println("Total Time: " + (System.nanoTime() - startTime) / 1e9D + "s")
  }
}
