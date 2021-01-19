import Utils.Utils
import org.apache.spark.sql.functions.{col, datediff, sum}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object SparkBatchTask {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\Users\\Iliia_Davkaev\\Downloads\\hadoop_bin\\hadoop-3.2.1")
    val KAFKA_SERVERS = "localhost:9092"
    val HDFS_DATA_EXPEDIA = "hdfs://127.0.0.1:8020/data/expedia/*.avro"

    val sparkSession = Utils.getSparkSession

    val hotels = getDataFromKafka(sparkSession, KAFKA_SERVERS, "hotels-weather_3")
      .selectExpr("CAST(value AS STRING) as hotel")
      .select(functions.from_json(functions.col("hotel"), Utils.getHotelsSchema).as("hotel_json"))
      .select("hotel_json.*")
      .distinct()

    val expedias = getDataFromHDSF(HDFS_DATA_EXPEDIA, sparkSession, "com.databricks.spark.avro")

    // Calculate total living days (how many days all guests spent in hotel).
    val hotelsTotalDays = getTotalDaysSpentInHotel(expedias)

    // Get unpopular hotels where people spent less than 2200 days.
    val unpopularHotels = getInvalidExpediaData(hotelsTotalDays, 2200)

    // Get popular hotels and join with hotels weather data
    val popularHotels = hotelsTotalDays
      .join(unpopularHotels, hotelsTotalDays.col("hotel_id") === unpopularHotels.col("hotel_id"), "left_anti")
      .select("hotel_id", "days_spent")
      .join(hotels, hotelsTotalDays("hotel_id") === hotels("id"), "left")

  }

  def getInvalidExpediaData(hotelsData: DataFrame, days: Int): DataFrame = {
    hotelsData.where(s"days_spent < $days")
  }

  def getTotalDaysSpentInHotel(expedia: DataFrame): DataFrame = {

    expedia.select("id", "hotel_id", "srch_ci", "srch_co")
      .withColumn("days_spent", datediff(col("srch_co"), col("srch_ci")))
      .select("hotel_id", "days_spent")
      .groupBy("hotel_id").agg(sum("days_spent") as "days_spent")
  }

  def getDataFromKafka(session: SparkSession, kafkaServers: String, topic: String): DataFrame = {
    session
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
  }

  def getDataFromHDSF(hdfsLocation: String, sparkSession: SparkSession, format: String): DataFrame = {
    sparkSession
      .read
      .format(format)
      .load(hdfsLocation)
  }

}
