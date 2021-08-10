import org.apache.spark.sql.functions.{col, expr, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scalafx.application.JFXApp
import scalafx.application.JFXApp.PrimaryStage
import scalafx.scene.Scene
import scalafx.scene.chart.{CategoryAxis, LineChart, NumberAxis, XYChart}
import scalafx.collections.ObservableBuffer
import scalafx.scene.layout.FlowPane

object TimeSeriePlot extends JFXApp {
  def getDataFrame(spark:SparkSession, country:String): Unit =
  {

  }

  def getLineChart(df:DataFrame, xColumn: String, yColumn: String): LineChart[String, Number] = {
    val xAxis = new CategoryAxis()
    xAxis.label = "Date"
    val yAxis = new NumberAxis()
    val data = ObservableBuffer(
      df.select(xColumn, yColumn).collect.toSeq
        map { case row => XYChart.Data[String, Number](row(0).toString, row(1).toString.toDouble) })


    val series = XYChart.Series[String, Number](data)
    val lineChart = new LineChart(xAxis, yAxis, ObservableBuffer(series))
    lineChart.title = yColumn
    lineChart.legendVisible = false
    lineChart
  }

  val spark = SparkSession
    .builder()
    .appName("hello hive")
    .config("spark.master", "local[*]")
    .enableHiveSupport()
    .getOrCreate()

  spark.sql("Use project2")

  val country = "US"
  val df = CountryPeaks.apply(country, spark)
  val ratioDF = df.withColumn("death_ratio", when((col("new_cases") === 0D), 0).otherwise(expr("new_deaths * 1.0 / new_cases")))

  val confChart = getLineChart(df, "Date", "new_cases")
  val deathChart = getLineChart(df, "Date", "new_deaths")
  val deathRatioChart = getLineChart(ratioDF, "Date", "death_ratio")
  stage = new PrimaryStage {
    title = country

    scene = new Scene(500, 800) {
      val pane = new FlowPane
      pane.children += confChart
      pane.children += deathChart
      pane.children += deathRatioChart
      root = pane
    }
  }
}
