package com.mozilla.telemetry

import com.mozilla.telemetry.heka.HekaFrame
import com.mozilla.telemetry.views.SyncView
import com.mozilla.telemetry.views.SyncPingConverter
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.JsonMethods._
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

class SyncViewTest extends FlatSpec with Matchers{
  // TODO REMOVE ME - TEST ONLY CODE -
  System.setProperty("hadoop.home.dir", "O:\\src\\moz\\hadoop-common-2.2.0-bin-master")
  // TODO END REMOVE ME

  "SyncPing payload" can "be serialized" in {

    val testPayload = parse("""{
       |  "type": "sync",
       |  "id": "824bb059-d22f-4930-b915-45580ecf463e",
       |  "creationDate": "2016-09-02T04:35:18.787Z",
       |  "version": 4,
       |  "application": {
       |    "architecture": "x86-64",
       |    "buildId": "20160831030224",
       |    "name": "Firefox",
       |    "version": "51.0a1",
       |    "displayVersion": "51.0a1",
       |    "vendor": "Mozilla",
       |    "platformVersion": "51.0a1",
       |    "xpcomAbi": "x86_64-msvc",
       |    "channel": "nightly"
       |  },
       |  "payload": {
       |    "when": 1472790916859,
       |    "uid": "123456789012345678901234567890",
       |    "took": 1918,
       |    "version": 1,
       |    "status": {
       |        "service": "error.sync.failed_partial",
       |        "sync": "error.login.reason.network"
       |    },
       |    "engines": [
       |      {
       |        "name": "clients",
       |        "took": 203
       |      },
       |      {
       |        "name": "passwords",
       |        "took": 123
       |      },
       |      {
       |        "name": "tabs",
       |        "incoming": {
       |          "applied": 2,
       |          "failed": 1
       |        },
       |        "outgoing": [
       |          {
       |            "sent": 1
       |          }
       |        ],
       |        "took": 624
       |      },
       |      {
       |        "name": "bookmarks",
       |        "took": 15,
       |        "failureReason": {
       |            "name": "nserror",
       |            "code": 2152398878
       |        },
       |        "status": "error.engine.reason.unknown_fail"
       |      },
       |      {
       |        "name": "forms",
       |        "outgoing": [
       |          {
       |            "sent": 1
       |          }
       |        ],
       |        "took": 250
       |      },
       |      {
       |        "name": "history",
       |        "outgoing": [
       |          {
       |            "sent": 6
       |          }
       |        ],
       |        "took": 249
       |      }
       |    ]
       |  }
       |}""".stripMargin)
    val sparkConf = new SparkConf().setAppName("SyncPing")
    sparkConf.setMaster(sparkConf.get("spark.master", "local[1]"))
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    try {
      val row = SyncPingConverter.payloadToRow(testPayload \ "payload")
      // Serialize this one row as Parquet
      val sqlContext = new SQLContext(sc)
      val rdd = sc.parallelize(row.toSeq)
      val schema = SyncView.buildSchema
      val dataframe = sqlContext.createDataFrame(rdd, schema)

      dataframe.show() // See the console her

      // verify the contents.
      dataframe.count() should be (1)
      val checkRow = dataframe.first()

      checkRow.getAs[Long]("when") should be (1472790916859L)
      checkRow.getAs[String]("uid") should be ("123456789012345678901234567890")
      checkRow.getAs[Long]("took") should be (1918)
      checkRow.getAs[Long]("version") should be (1)

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
      val dataframe = sqlContext.createDataFrame(sc.parallelize(rows.toSeq), SyncView.buildSchema)
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
