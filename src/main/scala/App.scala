import org.apache.spark.sql.SparkSession

object App {

  var spark:SparkSession = null

  def main(args: Array[String]): Unit = {
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
