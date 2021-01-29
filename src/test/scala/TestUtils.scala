import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.davkaev.Utils.Helper

object TestUtils {

  def getExpediaSchemaForTest: StructType = {
    DataTypes.createStructType(
      Array[StructField](
        DataTypes.createStructField("id", DataTypes.StringType, false),
        DataTypes.createStructField("hotel_id", DataTypes.LongType, false),
        DataTypes.createStructField("srch_ci", DataTypes.StringType, false),
        DataTypes.createStructField("srch_co", DataTypes.StringType, false),
        DataTypes.createStructField("srch_children_cnt", DataTypes.StringType, false)
      ))
  }

  def getDaysSpentInHotelDFSchema: StructType = {
    DataTypes.createStructType(
      Array[StructField](
        DataTypes.createStructField("hotel_id", DataTypes.StringType, false),
        DataTypes.createStructField("days_spent", DataTypes.LongType, true)
      ))
  }

  def getHotelsTestSchema: StructType = {
    Helper.getHotelsSchema
  }

}
