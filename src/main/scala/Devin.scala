import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{callUDF, lit, min}

import java.sql.Date
import java.text.SimpleDateFormat

object Devin {
  val spark: SparkSession = App.spark
  val date_format = new SimpleDateFormat("MM/dd/yy")
  import spark.implicits._

  def USFirstCaseDate(headers: Array[String], record: Array[String]): (String, String, Date) = {
    for (x <- 11 until record.length) {
      if (record(x) != "0") {
        return (record(6), record(7), new Date(date_format.parse(headers(x)).getTime))
      }
    }
    // if no confirmed cases
    return (record(6), record(7), null)
  }

  def internationalFirstCaseDate(headers: Array[String], record: Array[String]): (String, String, Date) = {
    for (x <- 4 until record.length) {
      if (record(x) != "0") {
        return (record(0), record(1), new Date(date_format.parse(headers(x)).getTime))
      }
    }
    // if no confirmed cases
    return (record(0), record(1), null)
  }

  def USFirstConfirmedDataFrame(): DataFrame = {
    val time_series_confirmed_rdd = spark.sparkContext.textFile("input/time_series_covid_19_confirmed_US.csv")
      .map(record => record.split(",\\s*(?=([^\"]*\"[^\"]*\")*[^\"]*$)"))

    val headers = time_series_confirmed_rdd.first()

    return time_series_confirmed_rdd
      .filter(record => record(0) != headers(0))
      .map(record => USFirstCaseDate(headers, record))
      .toDF("Province/State","Country/Region","First_Confirmed_Case")
  }

  def internationalFirstConfirmedDataFrame(): DataFrame = {
    val time_series_confirmed_rdd = spark.sparkContext.textFile("input/time_series_covid_19_confirmed.csv")
      .map(row => row.split(",\\s*(?=([^\"]*\"[^\"]*\")*[^\"]*$)"))

    val headers = time_series_confirmed_rdd.first

    return time_series_confirmed_rdd
      .filter(record => record(0) != headers(0))
      .map(record => internationalFirstCaseDate(headers, record))
      .toDF("Province/State","Country/Region","First_Confirmed_Case")
  }

  def showMortalityRates(): Unit = {
    import spark.implicits._

    val first_confirmed_cases = USFirstConfirmedDataFrame().union(internationalFirstConfirmedDataFrame())
      .groupBy( $"Country/Region")
      .agg(min($"First_Confirmed_Case"))

    first_confirmed_cases.createOrReplaceTempView("first_confirmed_cases")

    val covid_19_data = spark.read
      .option("header", true)
      .csv("input/covid_19_data.csv")

    covid_19_data.createOrReplaceTempView("covid_19_data")

    val regional_aggregates = spark.sql("SELECT `Country/Region`, row_number() OVER (ORDER BY SUM(deaths)/sum(Confirmed) DESC) as rank, ROUND(sum(deaths)/sum(Confirmed)*100,3) as death_rate, sum(Confirmed) as confirmed_cases" +
      " FROM covid_19_data GROUP BY `Country/Region` ORDER BY rank ASC")

    regional_aggregates.join(first_confirmed_cases, Seq( "Country/Region"), "inner")
      .orderBy("rank").withColumnRenamed("min(First_Confirmed_Case", "First_Confirmed_Case").show


    // convert results to RDD and persist
    // try to use dataset
    // broadcast regional aggregates variable and then join it with time_series_confirmed. just show the joined table.
  }
}
