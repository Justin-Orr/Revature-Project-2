import App.{sc, spark}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{broadcast, col, lit, monotonically_increasing_id, row_number}
import org.joda.time.{DateTime, DateTimeZone}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel._

object Justin {

  var confirmed_df:DataFrame = null

  /* Main function to be used */

  def findings(spark:SparkSession):Unit = {
    var start = getTime()

    create_data_frames()
    getTotalConfirmationsBySeason(spark)

    var end = getTime()
    var diff = end - start
    println("Time Elapsed: " + f"$diff%1.3f" + " seconds")
  }

  /* Use case Functions */


  def getTotalConfirmationsBySeason(spark:SparkSession): Unit = {

    //Create data frames based on all columns within a range based of the column headers of the table

    val winter_sum_df = get_seasonal_confirmations(spark,4,42,"winter_19_20")
    val spring_sum_df = get_seasonal_confirmations(spark,43,134,"spring_20")
    val summer_sum_df = get_seasonal_confirmations(spark,135,226,"summer_20")
    val fall_sum_df = get_seasonal_confirmations(spark,227,317,"fall_20")
    val winter_sum_df2 = get_seasonal_confirmations(spark,318,407,"winter_20_21")
    val spring_sum_df2 = get_seasonal_confirmations(spark,408,470,"spring_21")

    var colNum = Seq(1,471) //inclusive (country/region column and row number column)
    val confirmed_short_df = confirmed_df.select(colNum map confirmed_df.columns map col: _*)

    val df1 = winter_sum_df2.join(broadcast(spring_sum_df2), Seq("row_num"), "inner")
    val df2 = fall_sum_df.join(broadcast(df1), Seq("row_num"), "inner")
    val df3 = summer_sum_df.join(broadcast(df2), Seq("row_num"), "inner")
    val df4 = spring_sum_df.join(broadcast(df3), Seq("row_num"), "inner")
    val df5 = winter_sum_df.join(broadcast(df4), Seq("row_num"), "inner")
    val confirmed_cases_by_season_df = confirmed_short_df.join(broadcast(df5), Seq("row_num"), "inner")

    confirmed_cases_by_season_df.createOrReplaceTempView("seasonal_confirmed_view")

    val seasonal_confirmed = spark.sql("SELECT `Country/Region`, " +
      "sum(winter_19_20_total_confirmed) as winter_19_20_total_confirmed, " +
      "sum(spring_20_total_confirmed) as spring_20_total_confirmed, " +
      "sum(summer_20_total_confirmed) as summer_20_total_confirmed, " +
      "sum(fall_20_total_confirmed) as fall_20_total_confirmed, " +
      "sum(winter_20_21_total_confirmed) as winter_20_21_total_confirmed, " +
      "sum(spring_21_total_confirmed) as spring_21_total_confirmed " +
      "FROM seasonal_confirmed_view " +
      "GROUP BY `Country/Region` " +
      "ORDER BY `Country/Region` ASC")

    seasonal_confirmed.persist(MEMORY_ONLY) // To optimize our queries

    seasonal_confirmed.createOrReplaceTempView("seasonal_confirmed")

    println("Top 10 Countries w/ the Most Covid Confirmations by Season")
    print_top_10_confirmed("winter_19_20_total_confirmed")
    print_top_10_confirmed("spring_20_total_confirmed")
    print_top_10_confirmed("summer_20_total_confirmed")
    print_top_10_confirmed("fall_20_total_confirmed")
    print_top_10_confirmed("winter_20_21_total_confirmed")
    print_top_10_confirmed("spring_21_total_confirmed")


  }

  def print_top_10_confirmed(col:String): Unit = {
    spark.sql("SELECT `Country/Region`, " + col + " " +
      "FROM seasonal_confirmed ORDER BY " + col + " DESC LIMIT 10").show()
  }

  /**
   * Using the total covid_19 confirmations file, return a df based on the season specified by the range of column header dates.
   * Look at the file headers to see the dates represented as columns (use excel to quickly grab the indices for each season)
  */
  def get_seasonal_confirmations(spark:SparkSession, startIndex:Int, endIndex:Int, season:String):DataFrame = {
    var colNum = startIndex to endIndex //inclusive
    val confirmed_season_df = confirmed_df.select(colNum map confirmed_df.columns map col: _*)

    val confirmed_season_rdd = confirmed_season_df.rdd
    val confirmed_season_2Darray = confirmed_season_rdd.collect()
    val confirmed_season_array_row = new Array[Int](confirmed_season_2Darray.length)

    for(i <- 0 until confirmed_season_2Darray.length) {
      var total = 0;
      for(j <- 0 until confirmed_season_2Darray(i).length) {
        total = total + confirmed_season_2Darray(i)(j).toString.toInt
      }
      confirmed_season_array_row(i) = total
    }

    import spark.implicits._
    var df = sc.parallelize(confirmed_season_array_row).toDF(season + "_total_confirmed")
    df = add_row_numbers(df)
    return df
  }

  /* General Helper Functions */

  def import_data(fileName: String):DataFrame = {
    return spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("input/" + fileName)
  }

  def create_data_frames(): Unit = {
    confirmed_df = import_data("time_series_covid_19_confirmed.csv")
    confirmed_df = add_row_numbers(confirmed_df)
  }

  def add_row_numbers(df:DataFrame): DataFrame = {
    val windSpec = Window.partitionBy(lit(0))
      .orderBy(monotonically_increasing_id())
    return df.withColumn("row_num", row_number().over(windSpec))
  }

  def getTime(): Double = {
    return DateTime.now(DateTimeZone.UTC).getMillis() / 1000.0
  }

}
