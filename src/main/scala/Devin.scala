import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{callUDF, lit, min}

import java.sql.Date
import java.text.SimpleDateFormat

object Devin {
  val spark: SparkSession = App.spark
  val date_format = new SimpleDateFormat("MM/dd/yy")
  import spark.implicits._

  /* My function - generates table of covid mortality rates per country + the date of the first confirmed case for
   * each country.
   */
  def showMortalityRates(): Unit = {

    // combine international and US time series tables and get the first confirmed case for each country/region
    val first_confirmed_cases = USFirstConfirmedDataFrame()
      .union(internationalFirstConfirmedDataFrame())
      .groupBy( $"Country/Region")
      .agg(min($"First_Confirmed_Case"))

    first_confirmed_cases.createOrReplaceTempView("first_confirmed_cases")


    // read the csv with overall totals of deaths and confirmed cases and create a spark.sql view to query
    spark.read
      .option("header", true)
      .csv("input/covid_19_data.csv")
      .createOrReplaceTempView("covid_19_data")

    // query the covid_19_data table for rate, rank by rate. join with the other table to get first confirmed case
    spark.sql("SELECT `Country/Region`, row_number() OVER (ORDER BY SUM(deaths)/sum(Confirmed) DESC) as rank, ROUND(sum(deaths)/sum(Confirmed)*100,3) as fatality_rate, sum(Confirmed) as confirmed_cases" +
      " FROM covid_19_data GROUP BY `Country/Region` ORDER BY rank ASC")
      .join(first_confirmed_cases, Seq( "Country/Region"), "inner")
      .orderBy("rank")
      .withColumnRenamed("min(First_Confirmed_Case)", "First_Confirmed_Case")
      .show
  }


  /* Helper Functions */

  // get first date of a US record
  def USFirstCaseDate(headers: Array[String], record: Array[String]): (String, String, Date) = {
    for (x <- 11 until record.length) {
      if (record(x) != "0") {
        return (record(6), record(7), new Date(date_format.parse(headers(x)).getTime))
      }
    }
    // if no confirmed cases
    return (record(6), record(7), null)
  }

  // get first date of an international record
  def internationalFirstCaseDate(headers: Array[String], record: Array[String]): (String, String, Date) = {
    for (x <- 4 until record.length) {
      if (record(x) != "0") {
        return (record(0), record(1), new Date(date_format.parse(headers(x)).getTime))
      }
    }
    // if no confirmed cases
    return (record(0), record(1), null)
  }

  // reads US records and gets first date for each
  def USFirstConfirmedDataFrame(): DataFrame = {
    val time_series_confirmed_rdd = spark.sparkContext.textFile("input/time_series_covid_19_confirmed_US.csv")
      .map(record => record.split(",\\s*(?=([^\"]*\"[^\"]*\")*[^\"]*$)"))

    val headers = time_series_confirmed_rdd.first()

    return time_series_confirmed_rdd
      .filter(record => record(0) != headers(0))
      .map(record => USFirstCaseDate(headers, record))
      .toDF("Province/State","Country/Region","First_Confirmed_Case")
  }

  // reads international records and gets first date for each
  def internationalFirstConfirmedDataFrame(): DataFrame = {
    val time_series_confirmed_rdd = spark.sparkContext.textFile("input/time_series_covid_19_confirmed.csv")
      .map(row => row.split(",\\s*(?=([^\"]*\"[^\"]*\")*[^\"]*$)"))

    val headers = time_series_confirmed_rdd.first

    return time_series_confirmed_rdd
      .filter(record => record(0) != headers(0))
      .map(record => internationalFirstCaseDate(headers, record))
      .toDF("Province/State","Country/Region","First_Confirmed_Case")
  }

}
