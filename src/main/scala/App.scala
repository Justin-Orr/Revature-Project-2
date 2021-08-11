import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object App {

  var spark:SparkSession = null
  var sc:SparkContext = null

  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    spark = spark_session_init()
    sc = spark.sparkContext
    println("-- Created Spark Session --")
    spark.sparkContext.setLogLevel("ERROR")
    //spark_test()
    //test(spark)
    Justin.findings(spark)

  }

  def testToDF(spark:SparkSession): Unit = {
    //The toDF function only works after you create the spark session and use the following import statement.
    //The import also has to be in a different method from either main or the function that initializes the spark session
    import spark.implicits._
    val rdd1 = sc.parallelize(Seq(1,2))
    val df = rdd1.toDF()
    df.show()
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
