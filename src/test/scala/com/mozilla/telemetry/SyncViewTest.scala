package com.mozilla.telemetry

import com.mozilla.telemetry.heka.HekaFrame
import com.mozilla.telemetry.views.{SyncPingConverter, SyncView}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

class SyncViewTest extends FlatSpec with Matchers{
  // TODO REMOVE ME - TEST ONLY CODE -
  if (System.getProperty("os.name").toLowerCase().startsWith("windows"))
    System.setProperty("hadoop.home.dir", "O:\\src\\moz\\hadoop-common-2.2.0-bin-master")
  // TODO END REMOVE ME

  "Old Style SyncPing payload" can "be serialized" in {

    val sparkConf = new SparkConf().setAppName("SyncPing")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[1]"))
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    try {
      val row = SyncPingConverter.pingToRows(SyncViewTestPayloads.singleSyncPing)
      // Serialize this one row as Parquet
      val sqlContext = new SQLContext(sc)
      val rdd = sc.parallelize(row.toSeq)
      val dataframe = sqlContext.createDataFrame(rdd, SyncPingConverter.syncType)

      dataframe.show() // See the console here

      // verify the contents.
      dataframe.count() should be (1)
      val checkRow = dataframe.first()

      checkRow.getAs[String]("app_build_id") should be ("20160831030224")
      checkRow.getAs[String]("app_display_version") should be ("51.0a1")
      checkRow.getAs[String]("app_name") should be ("Firefox")
      checkRow.getAs[String]("app_version") should be ("51.0a1")

      checkRow.getAs[Long]("when") should be (1472790916859L)
      checkRow.getAs[String]("uid") should be ("123456789012345678901234567890")
      checkRow.getAs[Long]("took") should be (1918)

      val status = checkRow.getAs[GenericRowWithSchema]("status")
      status.getAs[String]("service") should be ("error.sync.failed_partial")
      status.getAs[String]("sync") should be ("error.login.reason.network")

      val engines = checkRow.getAs[mutable.WrappedArray[GenericRowWithSchema]]("engines")
      for ( engine <- engines ) {
        val name = engine.getAs[String]("name")
        val took = engine.getAs[Long]("took")
        val status = engine.getAs[String]("status")
        val failureReason = engine.getAs[GenericRowWithSchema]("failureReason")
        val incoming = engine.getAs[GenericRowWithSchema]("incoming")
        val outgoing = engine.getAs[mutable.WrappedArray[GenericRowWithSchema]]("outgoing")
        name match {
          case "clients" =>
            took should be (203)
            status should be (null)
            failureReason should be (null)
            incoming should be (null)
            outgoing should be (null)

          case "passwords" =>
            took should be (123)
            status should be (null)
            failureReason should be (null)
            incoming should be (null)
            outgoing should be (null)

          case "tabs" =>
            took should be (624)
            status should be (null)
            failureReason should be (null)
            incoming.getAs[Long]("applied") should be (2)
            incoming.getAs[Long]("failed") should be (1)
            outgoing.length should be (1)
            outgoing(0).getAs[Long]("sent") should be (1)

          case "bookmarks" =>
            took should be (15)
            status should be ("error.engine.reason.unknown_fail")
            failureReason.getAs[String]("name") should be ("nserror")
            failureReason.getAs[String]("value") should be ("2152398878")
            incoming should be (null)
            outgoing should be (null)

          case "forms" =>
            took should be (250)
            status should be (null)
            failureReason should be (null)
            incoming should be (null)
            outgoing.length should be (1)
            outgoing(0).getAs[Long]("sent") should be (1)

          case "history" =>
            took should be (249)
            status should be (null)
            failureReason should be (null)
            incoming should be (null)
            outgoing.length should be (1)
            outgoing(0).getAs[Long]("sent") should be (6)

          case _ =>
            fail("Unexpected engine name")
        }
      }
    } finally {
      sc.stop()
    }
  }

  "New Style SyncPing payload" can "be serialized" in {

    val sparkConf = new SparkConf().setAppName("SyncPing")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[1]"))
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    try {
      val row = SyncPingConverter.pingToRows(SyncViewTestPayloads.multiSyncPing)
      // Serialize the rows to Parquet
      val sqlContext = new SQLContext(sc)
      val rdd = sc.parallelize(row.toSeq)
      val dataframe = sqlContext.createDataFrame(rdd, SyncPingConverter.syncType)

      dataframe.show() // See the console here

      // verify the contents - there are 2 syncs in this single ping.
      val rows = dataframe.collect()
      rows.length should be (2)

      val firstSync = rows(0)

      firstSync.getAs[String]("app_build_id") should be ("20160907030427")
      firstSync.getAs[String]("app_display_version") should be ("51.0a1")
      firstSync.getAs[String]("app_name") should be ("Firefox")
      firstSync.getAs[String]("app_version") should be ("51.0a1")

      firstSync.getAs[Long]("when") should be (1473313854446L)
      firstSync.getAs[String]("uid") should be ("12345678912345678912345678912345")
      firstSync.getAs[Long]("took") should be (2277)

      firstSync.getAs[GenericRowWithSchema]("status") should be (null)

      val engines = firstSync.getAs[mutable.WrappedArray[GenericRowWithSchema]]("engines")
      for ( engine <- engines ) {
        val name = engine.getAs[String]("name")
        val took = engine.getAs[Long]("took")
        val status = engine.getAs[String]("status")
        val failureReason = engine.getAs[GenericRowWithSchema]("failureReason")
        val incoming = engine.getAs[GenericRowWithSchema]("incoming")
        val outgoing = engine.getAs[mutable.WrappedArray[GenericRowWithSchema]]("outgoing")
        name match {
          case "clients" =>
            took should be (468)
            status should be (null)
            failureReason should be (null)
            incoming should be (null)
            outgoing.length should be (1)
            outgoing(0).getAs[Long]("sent") should be (1)

          case "passwords" =>
            took should be (16)
            status should be (null)
            failureReason should be (null)
            incoming should be (null)
            outgoing should be (null)

          case "tabs" =>
            took should be (795)
            status should be (null)
            failureReason should be (null)
            incoming.getAs[Long]("applied") should be (2)
            incoming.getAs[Long]("reconciled") should be (1)
            outgoing.length should be (1)
            outgoing(0).getAs[Long]("sent") should be (1)

          case "bookmarks" =>
            took should be (0)
            status should be (null)
            failureReason should be (null)
            incoming should be (null)
            outgoing should be (null)

          case "forms" =>
            took should be (266)
            status should be (null)
            failureReason should be (null)
            incoming should be (null)
            outgoing.length should be (1)
            outgoing(0).getAs[Long]("sent") should be (2)

          case "history" =>
            took should be (514)
            status should be (null)
            failureReason should be (null)
            incoming.getAs[Long]("applied") should be (2)
            outgoing.length should be (1)
            outgoing(0).getAs[Long]("sent") should be (2)

          case _ =>
            fail("Unexpected engine name")
        }
      }
      // The second sync in this payload.
      val secondSync = rows(1)

      secondSync.getAs[String]("app_build_id") should be ("20160907030427")
      secondSync.getAs[String]("app_display_version") should be ("51.0a1")
      secondSync.getAs[String]("app_name") should be ("Firefox")
      secondSync.getAs[String]("app_version") should be ("51.0a1")

      secondSync.getAs[Long]("when") should be (1473313890947L)
      secondSync.getAs[String]("uid") should be ("12345678912345678912345678912345")
      secondSync.getAs[Long]("took") should be (484)

      secondSync.getAs[GenericRowWithSchema]("status") should be (null)

      val secondEngines = secondSync.getAs[mutable.WrappedArray[GenericRowWithSchema]]("engines")
      for ( engine <- secondEngines ) {
        val name = engine.getAs[String]("name")
        val took = engine.getAs[Long]("took")
        val status = engine.getAs[String]("status")
        val failureReason = engine.getAs[GenericRowWithSchema]("failureReason")
        val incoming = engine.getAs[GenericRowWithSchema]("incoming")
        val outgoing = engine.getAs[mutable.WrappedArray[GenericRowWithSchema]]("outgoing")
        name match {
          case "clients" =>
            took should be (249)
            status should be (null)
            failureReason should be (null)
            incoming should be (null)
            outgoing should be (null)

          case "passwords" =>
            took should be (0)
            status should be (null)
            failureReason should be (null)
            incoming should be (null)
            outgoing should be (null)

          case "tabs" =>
            took should be (16)
            status should be (null)
            failureReason should be (null)
            incoming should be (null)
            outgoing should be (null)

          case "bookmarks" =>
            took should be (0)
            status should be (null)
            failureReason should be (null)
            incoming should be (null)
            outgoing should be (null)

          case "forms" =>
            took should be (0)
            status should be (null)
            failureReason should be (null)
            incoming should be (null)
            outgoing should be (null)

          case "history" =>
            took should be (0)
            status should be (null)
            failureReason should be (null)
            incoming should be (null)
            outgoing should be (null)

          case _ =>
            fail("Unexpected engine name")
        }
      }

    } finally {
      sc.stop()
    }
  }

  "SyncPing records" can "be serialized" in {
    val sparkConf = new SparkConf().setAppName("SyncPing")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[1]"))
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    try {
      // Use an example framed-heka message. This is a copy of early Sync pings grabbed from
      // telemetry-2/20160831/telemetry/4/sync/Firefox/nightly/51.0a1/20160830030201/
      val hekaFileName = "/sync-ping.heka"
      val hekaURL = getClass.getResource(hekaFileName)
      println(s" URL = '${hekaURL}'")
      val input = hekaURL.openStream()
      val rows = HekaFrame.parse(input).flatMap(SyncView.messageToRow)

      // Serialize this data as Parquet
      val sqlContext = new SQLContext(sc)
      val dataframe = sqlContext.createDataFrame(sc.parallelize(rows.toSeq), SyncPingConverter.syncType)
      val tempFile = com.mozilla.telemetry.utils.temporaryFileName()
      dataframe.write.parquet(tempFile.toString)

      // Then read it back
      val data = sqlContext.read.parquet(tempFile.toString)

      // Apparently there are 190 syncs pings here - not sure how to verify that count, nor how to verify any of the
      // contents...
      data.count() should be (190)
    } finally {
      sc.stop()
    }
  }
}
