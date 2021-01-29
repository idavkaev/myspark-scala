package org.davkaev

import com.typesafe.config.ConfigFactory
import org.davkaev.Utils.Constants
import org.davkaev.Utils.Helper
import org.apache.spark.sql.functions.{col, datediff, sum}
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}


object SparkBatchTask {

  val sparkSession = Helper.getSparkSession
  val config = ConfigFactory.load("application.conf").getConfig("basics")

  def main(args: Array[String]): Unit = {

    val hotels = getDataFromKafka(sparkSession, config.getString("kafka.servers"), Constants.KAFKA_TOPIC_HOTELS_WEATHER)
      .selectExpr("CAST(value AS STRING) as hotel")
      .select(functions.from_json(functions.col("hotel"), Helper.getHotelsSchema).as("hotel_json"))
      .select("hotel_json.*")
      .distinct()

    val expedias = getDataFromHDSF(config.getString("hdfs.url"), sparkSession, "avro")

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
      .select(col("hotel_id").cast(StringType), col("days_spent").cast(LongType))
      .groupBy("hotel_id").agg(sum("days_spent") as "days_spent")
  }

  private def getDataFromKafka(session: SparkSession, kafkaServers: String, topic: String): DataFrame = {
    session
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
  }

  private def getDataFromHDSF(hdfsLocation: String, sparkSession: SparkSession, format: String): DataFrame = {
    sparkSession
      .read
      .format(format)
      .load(hdfsLocation)
  }

}
