package org.davkaev.Utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.davkaev.domain.ObjTypes

object Helper {
  def getHotelsSchema(): StructType = {
    getSchema(ObjTypes.HOTEL)
  }

  def getExpediaSchema(): StructType = {
    getSchema(ObjTypes.EXPEDIA)
  }

  private def getSchema(objectType: ObjTypes): StructType = {
    objectType match {
      case ObjTypes.HOTEL => {
        new StructType()
          .add("id", StringType)
          .add("address", StringType)
          .add("avgWeathers", ArrayType(new StructType()
            .add("date", StringType)
            .add("tmp_c", DoubleType)
            .add("tmp_f", DoubleType)))
          .add("city", StringType)
          .add("country", StringType)
          .add("hash", StringType)
          .add("name", StringType)
      }
      case ObjTypes.EXPEDIA => {
        new StructType()
          .add("id", LongType)
          .add("date_time", StringType)
          .add("site_name", IntegerType)
          .add("posa_continent", IntegerType)
          .add("user_location_country", IntegerType)
          .add("user_location_region", IntegerType)
          .add("user_location_city", IntegerType)
          .add("orig_destination_distance", DoubleType)
          .add("user_id", IntegerType)
          .add("is_mobile", IntegerType)
          .add("is_package", IntegerType)
          .add("channel", IntegerType)
          .add("srch_ci", StringType)
          .add("srch_co", StringType)
          .add("srch_adults_cnt", IntegerType)
          .add("srch_children_cnt", IntegerType)
          .add("srch_rm_cnt", IntegerType)
          .add("srch_destination_id", IntegerType)
          .add("srch_destination_type_id", IntegerType)
          .add("hotel_id", LongType)
      }
    }
  }

  def getSparkSession: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", true)
      .config("spark.sql.shuffle.partitions", 5)
      .config("spark.driver.maxResultSize", "1500m")
      .appName("KafkaKafka")
      .getOrCreate()
  }

}
