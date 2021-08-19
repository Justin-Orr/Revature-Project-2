import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.IntegerType

object LoadData {

  //def timeSerie(filePath: String, aggBy: String, tableName: String, )

  def apply(spark: SparkSession): Unit = {

    import spark.implicits._
    spark.sql("Create database if not exists project2")
    spark.sql("Use project2")
    val df1 = spark.read.option("header", true).csv("input/covid_19_data.csv")
    //df1.printSchema()
    val newDF = df1
      .drop("SNo")
      .distinct()
      .withColumnRenamed("Country/Region", "Country_Region")
      .withColumn("Confirmed", col("Confirmed").cast(IntegerType))
      .withColumn("ObservationDate", to_date(col("ObservationDate"), "M/d/y"))
      .withColumn("Deaths", col("Deaths").cast(IntegerType))
      .withColumn("Recovered", col("Recovered").cast(IntegerType))
      .drop("Last Update")
      .groupBy("Country_Region", "ObservationDate")
      .sum()
      .withColumnRenamed("sum(Confirmed)", "Confirmed")
      .withColumnRenamed("sum(Deaths)", "Deaths")
      .withColumnRenamed("sum(Recovered)", "Recovered")
      .repartition(col("Country_Region"))
    newDF.write.mode("overwrite").saveAsTable("Covid_19")

  }
}
