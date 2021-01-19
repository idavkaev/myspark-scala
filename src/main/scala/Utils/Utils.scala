package Utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object Utils {
  def getHotelsSchema: StructType = {
    DataTypes.createStructType(
      Array[StructField](
        DataTypes.createStructField("id", DataTypes.StringType, false),
        DataTypes.createStructField("address", DataTypes.StringType, false),
        DataTypes.createStructField("avgWeathers", DataTypes.StringType, false),
        DataTypes.createStructField("city", DataTypes.StringType, false),
        DataTypes.createStructField("country", DataTypes.StringType, false),
        DataTypes.createStructField("hash", DataTypes.StringType, false),
        DataTypes.createStructField("name", DataTypes.StringType, false)
      ))
  }

  def getSparkSession: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("KafkaKafka")
      .getOrCreate()
  }

}
