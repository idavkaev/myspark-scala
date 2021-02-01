package org.davkaev.service

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object Writer {

  val config = ConfigFactory.load("application.conf").getConfig("basics")

  def writeToConsole(df:DataFrame, numRows: Int = 20):Unit = {
    df.writeStream
      .format("console")
      .outputMode("update")
      .option("numRows",numRows)
      .start()
      .processAllAvailable()
  }

  def writeToFile(df: DataFrame): Unit = {
    df.
      writeStream
      .outputMode("overwrite")
      .format("avro")
      .option("path", config.getString("hdfs.dest.url"))
      .option("checkpointLocation", "/tmp")
      .start
      .processAllAvailable()
  }

  def writeForEach(df: DataFrame, folder: String = ""): Unit = {
    df
      .writeStream
      .outputMode("update")
      .foreachBatch { (batch: Dataset[Row], batchId: Long) => {
        batch
          .write
          .format("parquet")
          .mode("overwrite")
          .save(config.getString("hdfs.dest.url") + folder)
      }
      }.start()
      .processAllAvailable()
  }
}
