import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object App {

  var spark:SparkSession = null

  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    spark = spark_session_init()
    println("-- Created Spark Session --")
    spark.sparkContext.setLogLevel("ERROR")
    //spark_test()
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

  def spark_session_init(): SparkSession = {
    return SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
  }

  def spark_test(): Unit = {
    spark.sql("create table if not exists newone(id Int,name String) row format delimited fields terminated by ','");
    spark.sql("LOAD DATA LOCAL INPATH 'input/test.txt' INTO TABLE newone")
    spark.sql("SELECT * FROM newone").show()
    spark.sql("DROP table newone")
  }

}
