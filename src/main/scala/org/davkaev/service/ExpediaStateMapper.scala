package org.davkaev.service

import org.davkaev.Utils.{Constants, Helper}
import org.davkaev.domain.{ExpediaItem, ExpediaState}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}

object ExpediaStateMapper extends Serializable {

  val spark: SparkSession = Helper.getSparkSession
  import spark.implicits._

  def getStaysReport(df: DataFrame): DataFrame = {
    df
      .as[ExpediaItem]
      .groupByKey(_.hotelId)
      .mapGroupsWithState(GroupStateTimeout.NoTimeout())(updateState)
      .map(state => {
        val hotel_id = state.hotelId
        val standard_stays = state.stdcntr
        val extd_standard_stays = state.stdextcntr
        val short_stays = state.shrtcntr
        val long_stays = state.lngcntr
        val err_data = state.errcntr
        val most_popular_stay = state.getMostPopularStay()
        (hotel_id, standard_stays, extd_standard_stays, short_stays, long_stays, err_data, most_popular_stay)
      })
      .toDF(Constants.HOTEL_ID,
        Constants.STD_STAY,
        Constants.STD_EXTD_STAY,
        Constants.SHORT_STAY,
        Constants.LONG_STAY,
        Constants.ERR_DATA,
        Constants.MOST_POPULAR_STAY)
      .toDF()
  }

  def updateState(hotelId: Long,
                  inputs: Iterator[ExpediaItem],
                  oldState: GroupState[ExpediaState]): ExpediaState = {

    var state: ExpediaState =
      if (oldState.exists) oldState.get else ExpediaState(hotelId)

    for (input <- inputs) {
      if (input.stayType == null) {
        return state
      }
      var errcntr = state.errcntr
      var lngcntr = state.lngcntr
      var shrtcntr = state.shrtcntr
      var stdcntr = state.stdcntr
      var stdextcntr = state.stdextcntr
      input.stayType match {
        case Constants.ERR_DATA => errcntr = state.errcntr + 1
        case Constants.LONG_STAY => lngcntr = state.lngcntr + 1
        case Constants.SHORT_STAY => shrtcntr = state.shrtcntr + 1
        case Constants.STD_STAY => stdcntr = state.stdcntr + 1
        case Constants.STD_EXTD_STAY => stdextcntr = state.stdextcntr + 1
        case _ => println("No Match")
      }
      state = ExpediaState(input.hotelId,
        errcntr = errcntr,
        shrtcntr = shrtcntr,
        stdcntr = stdcntr,
        stdextcntr = stdextcntr,
        lngcntr = lngcntr)

      oldState.update(state)
    }
    state
  }
}
