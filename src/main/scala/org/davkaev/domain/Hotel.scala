package org.davkaev.domain

case class Hotel(address: String,
                 city: String,
                 country: String,
                 hash: String,
                 name: String,
                 id: String,
                 avgWeathers: Array[AvgWeather]) {

  def getAvgCelsWeather(): Double = {
    avgWeathers.toStream.map(avg => avg.tmp_c).sum / avgWeathers.length
  }

  def getAvgFarWeather(): Double = {
    avgWeathers.toStream.map(avg => avg.tmp_f).sum / avgWeathers.length
  }
}
