import com.holdenkarau.spark.testing.{DataFrameSuiteBase, SharedSparkContext}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers


class DataTesting extends FunSuite
  with SharedSparkContext
  with Checkers
  with DataFrameSuiteBase {

  var expediaData: DataFrame = null

  test("testing days spent in hotels") {

    val sourceDf = spark.createDataFrame(
      sc.textFile("src/test/resources/expedia_test_data.csv").map(f => Row.fromSeq(f.split(","))),
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

    assertDataFrameEquals(expediaData, expectedResult)
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
}
