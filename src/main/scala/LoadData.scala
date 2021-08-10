import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, to_date, to_timestamp, udf}
import org.apache.spark.sql.types.IntegerType

case class CountryNumber(val name: String, var number:Array[Int])

object LoadData {

  //def timeSerie(filePath: String, aggBy: String, tableName: String, )

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    spark.sql("Create database if not exists project2")
    spark.sql("Use project2")
    val df1 = spark.read.option("header", true).csv("input/covid_19_data.csv")
    //df1.printSchema()
    val newDF = df1.withColumnRenamed("Country/Region", "Country_Region")
      .withColumn("Confirmed", col("Confirmed").cast(IntegerType))
      .withColumn("ObservationDate", to_date(col("ObservationDate"), "M/d/y"))
      .withColumn("Deaths", col("Deaths").cast(IntegerType))
      .withColumn("Recovered", col("Recovered").cast(IntegerType))
      .drop("SNo")
      .distinct()
      .drop("Last Update")
      .groupBy("Country_Region", "ObservationDate")
      .sum()
      .withColumnRenamed("sum(Confirmed)", "Confirmed")
      .withColumnRenamed("sum(Deaths)", "Deaths")
      .withColumnRenamed("sum(Recovered)", "Recovered")
      .repartition($"Country_Region")
//    newDF.show(5)
//    //newDF.createOrReplaceTempView("CovidView")
//    spark.sql("SET hive.exec.dynamic.partition = true")
//    spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")
//    spark.sql("Drop table if exists Covid")
//    spark.sql("Create table if not exists Covid (Date Date, Confirmed int, Deaths int, Recovered int) partitioned by (Country_Region string)")
    newDF.write.mode("overwrite").saveAsTable("Covid_19")
//    spark.sql("Insert overwrite table Covid partition(Country_Region) select `Country/Region` Country_Region, ObservationDate Date, Confirmed, Deaths, Recovered from Covid_19 where date_format(ObservationDate ,'u') = 7")
//    spark.sql("Select * from Covid limit 5").show
    /* spark.sql("Create table covid_19 " +
      "(SNo Int, " +
      "ObservationDate Date, " +
      "`Province/State` String, " +
      "`Country/Region` String, " +
      "Confirmed Int, " +
      "Deaths Int, " +
      "Recovered Int, " +
      "LastUpdate date) " +
      "row format delimited fields terminated by ','")
    spark.sql("Load data local inpath 'input/covid_19_data_processed' overwrite into Table covid_19") */
//    val confirmedDF = spark.read.option("header", true).option("inferSchema", true).csv("input/time_series_covid_19_confirmed.csv")
//    val confirmedByCountryDF = confirmedDF.drop("Lat").drop("Long")
//      .groupBy("Country/Region")
//      .sum()
//
//    val confirmedByCountryRDD = confirmedByCountryDF.rdd.map(x => x.toSeq).map(x => (x.head.toString, x.tail.map(s => s.toString.toInt).toArray))
//    println(confirmedByCountryRDD.take(1).deep)
//
//    val countries = confirmedByCountryRDD.map(row => CountryNumber(row._1, row._2))
//
//    val countryDF = countries.toDF("Country/Region", "Confirmed")
//    countryDF.write.mode("overwrite").saveAsTable("country_confirmed")
//    spark.sql("select * from country_confirmed limit 5").show
  }
}
