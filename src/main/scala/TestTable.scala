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
    spark.sql("Select * from Covid_19 where date_format(ObservationDate, 'u') = 6 Order by `Country/Region`, ObservationDate").show

  }
}
