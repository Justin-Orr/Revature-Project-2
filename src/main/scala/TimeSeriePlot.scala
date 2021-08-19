import org.apache.spark.sql.functions.{col, expr, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scalafx.application.JFXApp
import scalafx.application.JFXApp.PrimaryStage
import scalafx.collections.ObservableBuffer
import scalafx.scene.Scene
import scalafx.scene.chart.{CategoryAxis, LineChart, NumberAxis, XYChart}
import scalafx.scene.layout.FlowPane
import scala.annotation.tailrec
import scala.io.StdIn

object TimeSeriePlot extends JFXApp {

  def isCountryValid(spark:SparkSession, country: String): Boolean = {
    val res = spark.sql(s"Select distinct country_region from covid_19 where country_region = '$country'")
    !res.collectAsList().isEmpty
  }

  @tailrec
  def getCountry(spark:SparkSession): String =
  {
    val country = StdIn.readLine("Please enter the country/region you are interested:\n")
    if (!isCountryValid(spark, country)) {
      println(s"$country is not available, please try another name.")
      getCountry(spark)
    } else country
  }

  //get x, y data from dataframe and generate line chart
  def getLineChart(df:DataFrame, xColumn: String, yColumn: String): LineChart[String, Number] = {
    val xAxis = new CategoryAxis()
    xAxis.label = "Date"
    val yAxis = new NumberAxis()
    val data = ObservableBuffer(
      df.select(xColumn, yColumn).collect.toSeq
        map (row => XYChart.Data[String, Number](row(0).toString, row(1).toString.toDouble)))

    val series = XYChart.Series[String, Number](data)
    val lineChart = new LineChart(xAxis, yAxis, ObservableBuffer(series))
    lineChart.title = yColumn
    lineChart.legendVisible = false
    lineChart.setPrefHeight(240)
    lineChart.setPrefWidth(400)
    lineChart
  }

  val spark: SparkSession = SparkSession
    .builder()
    .appName("hello hive")
    .config("spark.master", "local[*]")
    .enableHiveSupport()
    .getOrCreate()

  spark.sql("Create database if not exists project2")
  spark.sql("Use project2")

  //println("Loading Data ...")
  //LoadData.apply(spark)

  //get the country/region to plot data
  val country: String = getCountry(spark)

  //get data from the specific country
  val df: DataFrame = CountryPeaks.apply(country, spark)

  //calculate the death rate based on new case and new death
  val ratioDF: DataFrame = df.withColumn("death_rate", when((col("new_cases") === 0D), 0).otherwise(expr("new_deaths * 1.0 / new_cases")))

  //generate three line charts
  //confChart: line chart of weekly new confirmed cases
  //deathChart: line chart of weekly new deaths
  //deathRatioChart: line chart of weekly death rate
  val confChart: LineChart[String, Number] = getLineChart(df, "Date", "new_cases")
  val deathChart: LineChart[String, Number] = getLineChart(df, "Date", "new_deaths")
  val deathRatioChart: LineChart[String, Number] = getLineChart(ratioDF, "Date", "death_rate")

  stage = new PrimaryStage {
    title = country
    scene = new Scene(420, 750) {
      val pane = new FlowPane
      pane.children += confChart
      pane.children += deathChart
      pane.children += deathRatioChart
      root = pane
    }
  }
}
