package org.davkaev.domain

import org.davkaev.Utils.Constants

case class ExpediaState(hotelId: Long,
                        stdcntr: Int = 0,
                        stdextcntr: Int = 0,
                        shrtcntr: Int = 0,
                        lngcntr: Int = 0,
                        errcntr: Int = 0
                       ) extends Serializable {

  def getMostPopularStay(): String = {
    val stays = Seq(shrtcntr, stdextcntr, stdcntr, errcntr, lngcntr)

    stays.max match {
      case `stdcntr` => Constants.STD_STAY
      case `stdextcntr` => Constants.STD_EXTD_STAY
      case `lngcntr` => Constants.LONG_STAY
      case `shrtcntr` => Constants.SHORT_STAY
      case `errcntr` => Constants.ERR_DATA
      case _ => "UNKNOWN"
    }
  }
}
