import App.{sc, spark}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{broadcast, col, lit, monotonically_increasing_id, row_number}
import org.joda.time.{DateTime, DateTimeZone}

import Array._


object Justin {

  var c19_df:DataFrame = null
  var confirmed_df:DataFrame = null
  var confirmed_US_df:DataFrame = null
  var deaths_df:DataFrame = null
  var deaths_US_df:DataFrame = null
  var recovered_df:DataFrame = null

  /* Main function to be used */

  def findings(spark:SparkSession):Unit = {
    create_data_frames()
    getTotalConfirmationsBySeason(spark)
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
    //c19_df = import_data("covid_19_data.csv")

    confirmed_df = import_data("time_series_covid_19_confirmed.csv")
    confirmed_df = add_row_numbers(confirmed_df)

    //confirmed_US_df = import_data("time_series_covid_19_confirmed_US.csv")
    //deaths_df = import_data("time_series_covid_19_deaths.csv")
    //deaths_US_df = import_data("time_series_covid_19_deaths_US.csv")
    //recovered_df = import_data("time_series_covid_19_recovered.csv")
  }

  def add_row_numbers(df:DataFrame): DataFrame = {
    val windSpec = Window.partitionBy(lit(0))
      .orderBy(monotonically_increasing_id())
    return df.withColumn("row_num", row_number().over(windSpec))
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

    var colNum = Seq(0,1,2,3,471) //inclusive
    val confirmed_short_df = confirmed_df.select(colNum map confirmed_df.columns map col: _*)


    var start = getTime()
    val df1 = winter_sum_df2.join(broadcast(spring_sum_df2), Seq("row_num"), "inner")
    val df2 = fall_sum_df.join(broadcast(df1), Seq("row_num"), "inner")
    val df3 = summer_sum_df.join(broadcast(df2), Seq("row_num"), "inner")
    val df4 = spring_sum_df.join(broadcast(df3), Seq("row_num"), "inner")
    val df5 = winter_sum_df.join(broadcast(df4), Seq("row_num"), "inner")
    val confirmed_cases_by_season_df = confirmed_short_df.join(broadcast(df5), Seq("row_num"), "inner")
    confirmed_cases_by_season_df.show(10)

    var end = getTime()
    var diff = end - start
    println("Time Elapsed: " + f"$diff%1.3f" + " seconds")

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

  def getTime(): Double = {
    return DateTime.now(DateTimeZone.UTC).getMillis() / 1000.0
  }

}
