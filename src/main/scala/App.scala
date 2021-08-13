
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

  def spark_test() {

    // LOAD DATA INTO DATAFRAMES
    val df_covid_19_data = spark.read.format("csv").option("header", "true").load("input/covid_19_data.csv")
    val df_confirmed = spark.read.format("csv").option("header", "true").load("input/time_series_covid_19_confirmed.csv")
    val df_confirmed_US = spark.read.format("csv").option("header", "true").load("input/time_series_covid_19_confirmed_US.csv")
    val df_deaths = spark.read.format("csv").option("header", "true").load("input/time_series_covid_19_deaths.csv")
    val df_deaths_US = spark.read.format("csv").option("header", "true").load("input/time_series_covid_19_deaths_US.csv")
    val df_recovered = spark.read.format("csv").option("header", "true").load("input/time_series_covid_19_recovered.csv")

    //df_covid_19_data.select("Province/State").show()

    //CONVERT DATAFRAMES INTO SPARK SQL TABLES
    df_covid_19_data.createOrReplaceTempView("tb_covid_19_data")
    df_confirmed.createOrReplaceTempView("tb_confirmed")
    df_confirmed_US.createOrReplaceTempView("tb_confirmed_US")
    df_deaths.createOrReplaceTempView("tb_deaths")
    df_deaths_US.createOrReplaceTempView("tb_deaths_US")
    df_recovered.createOrReplaceTempView("tb_recovered")


    // SPARK SQL QUERIES
    //spark.sql("update tb_confirmed set `Counrty/Region` = Replace(`Country/Region`,'China','Mainland China')")
    spark.sql("drop database if exists db_p2 cascade")
    spark.sql("create database db_p2")
    spark.sql("use db_p2")
    spark.sql("select `Country/Region`,sum(Confirmed) as Confirmed,sum(Deaths) as Deaths from tb_covid_19_data group by `Country/Region`")createOrReplaceTempView("tb_5")
    spark.sql("select * from tb_5").show()
    spark.sql("SELECT tb_5.`Country/Region`,tb_confirmed.Lat,`Confirmed`,`Deaths` from tb_5 right join tb_confirmed on tb_5.`Country/Region` = tb_confirmed.`Country/Region` order by Lat asc")createOrReplaceTempView("tb_6")
    spark.sql("select `Country/Region`, lat, confirmed, deaths")
    //spark.sql("create table tb1 as SELECT tb_covid_19_data.`Province/State`,tb_covid_19_data.`Country/Region`,tb_confirmed.Lat,`Confirmed`,`Deaths`,`Recovered` from tb_covid_19_data right join tb_confirmed on tb_covid_19_data.`Country/Region` = tb_confirmed.`Country/Region` or tb_covid_19_data.`Country/Region` = 'Mainland ' + tb_confirmed.`Country/Region` order by Lat ASC").show()
   // spark.sql("update tb1 set `Counrty/Region` = Replace(`Country/Region`,'China','Mainland China')")
    //spark.sql("create table tb2 as SELECT REPLACE('tb1','Mainland China','China'")
    //spark.sql("select * from tbg7").show()
    //spark.sql("SELECT tb_covid_19_data.`Province/State`,tb_covid_19_data.`Country/Region`,tb_confirmed.Lat,`Confirmed`,`Deaths`,`Recovered` from tb_covid_19_data right join tb_confirmed on tb_covid_19_data.`Country/Region` = tb_confirmed.`Country/Region` order by Lat ASC")
    //spark.sql("ALTER TABLE tb1 ADD LatitudeGroup varchar(255)").show()
    //spark.sql("ALTER TABLE tb_covid_19_data MODIFY COLUMN Lat string")
    //spark.sql("select * from tb_covid_19_data").show()

    //spark.sql("select `Country/Region`, sum(Confirmed), sum(Deaths) from tb_covid_19_data group by `Country/Region` ").show()
    //spark.sql("select `Province/State` from tb_covid_19_data").show()

    //spark.sql("create table if not exists newone(id Int,name String) row format delimited fields terminated by ','");
    //spark.sql("LOAD DATA LOCAL INPATH 'input/test.txt' INTO TABLE newone")
    //spark.sql("SELECT * FROM tb1").show()
    //spark.sql("DROP table newone")
  }
}
