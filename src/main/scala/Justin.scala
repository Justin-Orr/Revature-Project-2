import App.spark
import org.apache.spark.sql.DataFrame

object Justin {

  def findings():Unit = {
    val c19_df = import_data("covid_19_data.csv")
    c19_df.show(10)

    val c19_confirmed_df = import_data("time_series_covid_19_confirmed.csv")
    c19_confirmed_df.show(10)

    val c19_confirmed_US_df = import_data("time_series_covid_19_confirmed_US.csv")
    c19_confirmed_US_df.show(10)

    val c19_deaths_df = import_data("time_series_covid_19_deaths.csv")
    c19_deaths_df.show(10)

    val c19_deaths_US_df = import_data("time_series_covid_19_deaths_US.csv")
    c19_deaths_US_df.show(10)

    val c19_recovered_df = import_data("time_series_covid_19_recovered.csv")
    c19_recovered_df.show(10)
  }

  def import_data(fileName: String):DataFrame = {
    return spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("input/" + fileName)
  }

}
