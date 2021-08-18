/*
Create a Spark Application that process Covid data.
Your project  should involve useful analysis of covid data
(Every concept of spark like rdd, dataframes, sql, dataset and optimization methods  and  persistence should be included).
The Expected output is different trends that you have observed as part of data collectively and how you can use these trends to make some useful decisions.
Let the P2, have presentation with screen shots and practical demo.
 */
//import Mark.spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
object App {
  var spark:SparkSession = null
  //import spark.implicits._
  var sc:SparkContext = null
  //var sc = spark.sparkContext;
  //import spark.implicits._


  def main(args: Array[String]) : Unit = {
    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    spark = spark_session_init()
    sc = spark.sparkContext
    println("-- Created Spark Session --")

    spark.sparkContext.setLogLevel("ERROR")
    //spark_test()
    Mark.show_tables(spark)


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

  }
}
