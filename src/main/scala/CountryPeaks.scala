import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{broadcast, col, date_add, expr, lag, udf, when}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object CountryPeaks {
  def countryWeeklyBroadCast(name:String, spark:SparkSession): DataFrame = {
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

  def countryWeeklyWindow(name:String, spark:SparkSession): DataFrame = {
    try {
      val windowSpec = Window.orderBy("Date")
      val weeklyTotal = spark.sql(s"Select * from covid_19 where Country_Region = '$name' " +
        s"and date_format(ObservationDate, 'u') = 6 order by ObservationDate")
        .toDF("Country_Region", "Date", "Confirmed", "Deaths", "Recovered")

      val weeklyNew = weeklyTotal.withColumn("prev_cases", lag("Confirmed", 1, 0) over windowSpec)
        .withColumn("prev_deaths", lag("Deaths", 1, 0) over windowSpec)
        .withColumn("prev_recovered", lag("Recovered", 1, 0) over windowSpec)
        .withColumn("new_cases", expr("Confirmed - prev_cases"))
        .withColumn("new_deaths", expr("Deaths - prev_deaths"))
        .withColumn("new_recovered", expr("Recovered - prev_recovered"))
        .select("Date", "new_cases", "new_deaths", "new_recovered")

      weeklyNew
    }
    catch {
      case ex: ParseException => null
    }
  }

  def countryWeeklySQLWindow(name:String, spark:SparkSession): DataFrame = {
    try {
      val weeklyNew = spark.sql(s"Select ObservationDate Date, " +
        s"Confirmed - lag(Confirmed, 1, 0) over (Order by ObservationDate asc) new_cases, " +
        s"Deaths - lag(Deaths, 1, 0) over (Order by ObservationDate) new_deaths, " +
        s"Recovered - lag(Recovered, 1, 0) over (Order by ObservationDate) new_recovered " +
        s"from covid_19 where Country_Region = '$name' " +
        s"and date_format(ObservationDate, 'u') = 6")

      weeklyNew
    }
    catch {
      case ex: ParseException =>
        {
          ex.printStackTrace()
          null
        }
    }
  }

  def countryWeekly(name:String, spark:SparkSession): DataFrame = {
    try {
      val weeklyTotal = spark.sql(s"Select * from covid_19 where Country_Region = '$name' " +
        s"and date_format(ObservationDate, 'u') = 6 order by ObservationDate")
        .toDF("Country_Region", "Date", "Confirmed", "Deaths", "Recovered")

      val broadcastWeekly = weeklyTotal
        .withColumn("prevWeek", date_add(col("Date"), 7))
        .select(col("prevWeek"),
          col("Confirmed").as("prev_Confirmed"),
          col("Deaths").as("prev_Deaths"),
          col("Recovered").as("prev_Recovered"))

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

  def apply(country: String, spark:SparkSession): DataFrame = countryWeeklySQLWindow(country, spark)

  def testMethod(method: (String, SparkSession)=> DataFrame, countries:Array[Row], spark: SparkSession): String = {
    val startTime = System.nanoTime()
    for (country<-countries) {
      try {
        method(country(0).toString, spark).collect()
      }
      catch {
        case ex: NullPointerException => {}
      }
    }
    "Total Time: " + (System.nanoTime() - startTime) / 1e9D + "s"
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("hello hive")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("Use project2")
    val countries: Array[Row] = spark.sql("Select distinct Country_Region from covid_19").collect()


    val t1 = testMethod(countryWeekly, countries, spark)
    val t2 = testMethod(countryWeeklyBroadCast, countries, spark)
    val t3 = testMethod(countryWeeklyWindow, countries, spark)
    val t4 = testMethod(countryWeeklySQLWindow, countries, spark)

    println("Self join without broadcasting variable: " + t1)
    println("Self join with broadcasting variable: " + t2)
    println("Dataframe window function: " + t3)
    println("SQL window function: " + t4)


  }
}
