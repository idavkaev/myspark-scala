import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}
import org.davkaev.{SparkBatchTask, SparkStreamingTask}
import org.scalatest.prop.Checkers
import org.scalatest.{Assertions, FunSuite}
import org.davkaev.service.ExpediaStateMapper


class Testings extends FunSuite
  with SharedSparkContext
  with Checkers
  with DataFrameSuiteBase {

  var expediaData: DataFrame = null

  test("testing days spent in hotels") {

    val sourceDf = spark.createDataFrame(
      sc.textFile("src/test/resources/expedia_test_data.csv").map(f => {
        var line = f.split(",")
        Row(line(0), line(1).toLong, line(2), line(3), line(4))
      }),
      TestUtils.getExpediaSchemaForTest
    )

    expediaData = SparkBatchTask.getTotalDaysSpentInHotel(sourceDf)

    val expectedResult = spark.createDataFrame(
      sc.textFile("src/test/resources/days_spent_in_hotels.csv").map(f => {
        var line = f.split(",")
        Row(line(0), line(1).toLong)
      }),
      TestUtils.getDaysSpentInHotelDFSchema
    )

    assertDataFrameDataEquals(expediaData, expectedResult)
  }

  test("test getting valid expedia data") {
    val result = SparkBatchTask.getInvalidExpediaData(expediaData, 15)
    val expectedResult = spark.createDataFrame(
      sc.textFile("src/test/resources/invalid_hotels_data.csv").map(f => {
        var line = f.split(",")
        Row(line(0), line(1).toLong)
      }),
      TestUtils.getDaysSpentInHotelDFSchema
    )

    assertDataFrameDataEquals(result, expectedResult)
  }

  test("Testing DF with and without children") {

    val sourceDf = spark.createDataFrame(
      sc.textFile("src/test/resources/expedia_test_data.csv").map(f => {
        var line = f.split(",")
        Row(line(0), line(1).toLong, line(2), line(3), line(4))
      }),
      TestUtils.getExpediaSchemaForTest
    )

    intercept[org.scalatest.exceptions.TestFailedException] {
      assertDataFrameDataEquals(SparkStreamingTask.getExpediasGroupedByStayDays(sourceDf, true),
        SparkStreamingTask.getExpediasGroupedByStayDays(sourceDf, false))
    }
    Assertions.assert(SparkStreamingTask.getExpediasGroupedByStayDays(sourceDf, false).count() == 32)
  }

  test("testing stay type correctness") {

    val sourceDf = spark.createDataFrame(
      sc.textFile("src/test/resources/expedia_test_data.csv").map(f => {
        var line = f.split(",")
        Row(line(0), line(1).toLong, line(2), line(3), line(4))
      }),
      TestUtils.getExpediaSchemaForTest
    )
    val df = ExpediaStateMapper.getStaysReport(SparkStreamingTask.getExpediasGroupedByStayDays(sourceDf, false))

    assertDataFrameDataEquals(df.where(col("most_popular_stay") === "err_data"), df.where(col("hotel_id") === 3))
    assertDataFrameDataEquals(df.where(col("most_popular_stay") === "short_stay"), df.where(col("hotel_id") isin(2, 9)))
  }
}
