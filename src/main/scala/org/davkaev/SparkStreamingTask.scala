package org.davkaev

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.{col, datediff, when}
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.davkaev.Utils.{Constants, Helper}
import org.davkaev.domain.Hotel
import org.davkaev.service.{ExpediaStateMapper, Writer}

object SparkStreamingTask {

  val config = ConfigFactory.load("application.conf").getConfig("basics")
  val sparkSession = Helper.getSparkSession
  import sparkSession.implicits._

  def main(args: Array[String]): Unit = {

    val hotels = getDataFromKafka(sparkSession, config.getString("kafka.servers"), Constants.KAFKA_TOPIC_HOTELS_WEATHER)
    val expedias = getDataFromHDSF(config.getString("hdfs.source.url"), sparkSession, "avro")

    val expediaStaysNoChildren = getExpediasGroupedByStayDays(expedias, false)
    val expediaStaysWithChildren = getExpediasGroupedByStayDays(expedias, true)

    val hotelsAvgWeathers = getHotelsWithAvgWeather(hotels)

    val resultNoChildren = ExpediaStateMapper.getStaysReport(expediaStaysNoChildren)
      .join(hotelsAvgWeathers.where(col(Constants.AVG_CELS_WEATHER) < 20), Seq(Constants.HOTEL_ID))

    val resultChildren = ExpediaStateMapper.getStaysReport(expediaStaysWithChildren)
      .join(hotelsAvgWeathers, Seq(Constants.HOTEL_ID))

    //    Writer.writeToConsole(resultNoChildren)
    //    Writer.writeToConsole(resultNoChildren)
    Writer.writeForEach(resultChildren, "with_children")
    Writer.writeForEach(resultNoChildren, "no_children")
  }

  def getExpediasGroupedByStayDays(df: DataFrame, withChildren: Boolean): DataFrame = {
    df
      .select(col("hotel_id").alias("hotelId"),
        when(datediff(col("srch_co"), col("srch_ci")) between(2, 7), Constants.STD_STAY)
          .when(datediff(col("srch_co"), col("srch_ci")) === 1, Constants.SHORT_STAY)
          .when(datediff(col("srch_co"), col("srch_ci")) between(7, 14), Constants.STD_EXTD_STAY)
          .when(datediff(col("srch_co"), col("srch_ci")) between(14, 28), Constants.LONG_STAY)
          .otherwise(Constants.ERR_DATA)
          .alias("stayType")
      )
      .where(col("srch_children_cnt").cast(BooleanType) === withChildren)
  }

  def getHotelsWithAvgWeather(df: DataFrame) = {

    df
      .selectExpr("CAST(value AS STRING) as hotel")
      .select(functions.from_json(functions.col("hotel"), Helper.getHotelsSchema()).as("hotel_json"))
      .select("hotel_json.*")
      .distinct()
      .as[Hotel]
      .map(hotel => (hotel.id, hotel.country, hotel.city, hotel.address, hotel.getAvgCelsWeather()))
      .toDF(Constants.HOTEL_ID, Constants.COUNTRY, Constants.CITY, Constants.ADDRESS, Constants.AVG_CELS_WEATHER)
  }

  private def getDataFromKafka(session: SparkSession, kafkaServers: String, topic: String): DataFrame = {
    session
      .read // batch reading because we cannot join two streams
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
  }

  private def getDataFromHDSF(hdfsLocation: String, sparkSession: SparkSession, format: String): DataFrame = {
    sparkSession
      .readStream
      .format(format)
      .schema(Helper.getExpediaSchema())
      .load(hdfsLocation)
  }

}
