
import org.apache.spark.sql.SparkSession
object App {
  var spark:SparkSession = null
  //var sc = spark.sparkContext;


  def main(args: Array[String]) : Unit = {
    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    spark = spark_session_init()
    println("-- Created Spark Session --")

    spark.sparkContext.setLogLevel("ERROR")
    spark_test()


  }

  def spark_session_init(): SparkSession = {
    return SparkSession
      .builder
      .appName("Hello Hive...")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
  }

  def spark_test()  {

    // LOAD DATA INTO DATAFRAMES
    val df_covid_19_data = spark.read.format("csv").option("header", "true").load("input/covid_19_data.csv")
    val df_confirmed = spark.read.format("csv").option("header", "true").load("input/time_series_covid_19_confirmed.csv")
    val df_confirmed_US = spark.read.format("csv").option("header", "true").load("input/time_series_covid_19_confirmed_US.csv")
    val df_deaths = spark.read.format("csv").option("header", "true").load("input/time_series_covid_19_deaths.csv")
    val df_deaths_US = spark.read.format("csv").option("header", "true").load("input/time_series_covid_19_deaths_US.csv")
    val df_recovered = spark.read.format("csv").option("header", "true").load("input/time_series_covid_19_recovered.csv")

    // CONVERT DATAFRAMES INTO SPARK SQL TABLES
    df_covid_19_data.createOrReplaceTempView("tb_covid_19_data")
    df_confirmed.createOrReplaceTempView("tb_confirmed")
    df_confirmed_US.createOrReplaceTempView("tb_confirmed_US")
    df_deaths.createOrReplaceTempView("tb_deaths")
    df_deaths_US.createOrReplaceTempView("tb_deaths_US")
    df_recovered.createOrReplaceTempView("tb_recovered")

    // SPARK SQL QUERIES TO CREATE LATITUDE TABLE
    spark.sql("drop database if exists db_p2 cascade")
    spark.sql("create database db_p2")
    spark.sql("use db_p2")
    spark.sql("select `Country/Region`,sum(Confirmed) as Confirmed,sum(Deaths) as Deaths from tb_covid_19_data group by `Country/Region`")createOrReplaceTempView("tb_5")
    spark.sql("select * from tb_5").show()
    spark.sql("SELECT tb_5.`Country/Region`,tb_confirmed.Lat,`Confirmed`,`Deaths` from tb_5 right join tb_confirmed on tb_5.`Country/Region` = tb_confirmed.`Country/Region` order by Lat asc")createOrReplaceTempView("tb_6")
    spark.sql("select `Country/Region`, lat, confirmed, deaths, case when (Lat >= 0 and Lat <= 30) then ' 0  to  30' when (Lat > 30 and Lat <= 60) then ' 30  to  60' when (Lat > 60 and Lat <= 90) then ' 60  to  90' when (Lat < 0 and Lat >= -30) then '-30  to   0' when (Lat < -30 and Lat >= -60) then '-60  to -30' when (Lat < -60 and Lat >= -90) then '-90  to -60' when (`Country/Region` = 'China') then ' 30  to  60' when (`Country/Region` = 'Canada') then ' 30  to  60' else 'unknown' end as Lat_Group from tb_6")createOrReplaceTempView("tb_8")
    println("Table Data From 1/22/2020 to 5/2/2021")
    spark.sql("select Lat_Group, round(sum(confirmed)/1000000,2) as Confirmed_Millions, round(sum(deaths)/1000000,2) as Deaths_Millions, concat(cast(round(sum(deaths)/sum(confirmed)*100, 3) as string), ' %')  as Death_Rate from tb_8 group by Lat_Group order by Lat_Group ASC").show()

    // CONVERT TABLE QUERY TO DATAFRAME
    val df_lat = spark.sql("select Lat_Group, round(sum(confirmed)/1000000,2) as Confirmed_Millions, round(sum(deaths)/1000000,2) as Deaths_Millions, concat(cast(round(sum(deaths)/sum(confirmed)*100, 3) as string), ' %')  as Death_Rate from tb_8 group by Lat_Group order by Lat_Group ASC").show()

    // CONVERT TO RDD

    // CONVERT TO DATASET

    // PERSIST

  }
}
