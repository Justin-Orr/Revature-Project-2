import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, to_date, to_timestamp, udf}
import org.apache.spark.sql.types.IntegerType

object TestTable {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("Use project2")
    print(spark.sql("Select distinct country_region from Covid_19 where country_region = 'US'").collectAsList().isEmpty)

  }
}
