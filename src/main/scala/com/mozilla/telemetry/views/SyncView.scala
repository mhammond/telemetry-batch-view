package com.mozilla.telemetry.views

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Days, format}
import org.json4s.JsonAST.{JValue, _}
import org.json4s.jackson.JsonMethods.parse
import org.json4s.DefaultFormats
import org.rogach.scallop._
import com.mozilla.telemetry.heka.{Dataset, Message}
import com.mozilla.telemetry.utils.{MainPing, S3Store}
import com.mozilla.telemetry.heka.HekaFrame
import org.json4s.JValue // Just for my attempted mocks below.....

object SyncView {
  def streamVersion: String = "v1"
  def jobName: String = "sync_summary"

  // Configuration for command line arguments
  private class Conf(args: Array[String]) extends ScallopConf(args) {
    val from = opt[String]("from", descr = "From submission date", required = false)
    val to = opt[String]("to", descr = "To submission date", required = false)
    val outputBucket = opt[String]("bucket", descr = "Destination bucket for parquet data", required = true)
    val limit = opt[Int]("limit", descr = "Maximum number of files to read from S3", required = false)
    val channel = opt[String]("channel", descr = "Only process data from the given channel", required = false)
    val appVersion = opt[String]("version", descr = "Only process data from the given app version", required = false)
    verify()
  }

  def main(args: Array[String]) {
    val conf = new Conf(args) // parse command line arguments
    val fmt = format.DateTimeFormat.forPattern("yyyyMMdd")
    val to = conf.to.get match {
      case Some(t) => fmt.parseDateTime(t)
      case _ => DateTime.now.minusDays(1)
    }
    val from = conf.from.get match {
      case Some(f) => fmt.parseDateTime(f)
      case _ => DateTime.now.minusDays(1)
    }

    // XXX - the below is copied verbatim from MainSummaryView - I've no idea how much is relevant here.
    // Set up Spark
    val sparkConf = new SparkConf().setAppName(jobName)
    sparkConf.setMaster(sparkConf.get("spark.master", "local[*]"))
    implicit val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    // We want to end up with reasonably large parquet files on S3.
    val parquetSize = 512 * 1024 * 1024
    hadoopConf.setInt("parquet.block.size", parquetSize)
    hadoopConf.setInt("dfs.blocksize", parquetSize)
    // Don't write temp files to S3 while building parquet files.
    hadoopConf.set("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.DirectParquetOutputCommitter")
    // Don't write metadata files, because they screw up partition discovery.
    // This is fixed in Spark 2.0, see:
    //   https://issues.apache.org/jira/browse/SPARK-13207
    //   https://issues.apache.org/jira/browse/SPARK-15454
    //   https://issues.apache.org/jira/browse/SPARK-15895
    hadoopConf.set("parquet.enable.summary-metadata", "false")

    for (offset <- 0 to Days.daysBetween(from, to).getDays) {
      val currentDate = from.plusDays(offset)
      val currentDateString = currentDate.toString("yyyyMMdd")
      val filterChannel = conf.channel.get
      val filterVersion = conf.appVersion.get

      println("=======================================================================================")
      println(s"BEGINNING JOB $jobName FOR $currentDateString")
      if (filterChannel.nonEmpty)
        println(s" Filtering for channel = '${filterChannel.get}'")
      if (filterVersion.nonEmpty)
        println(s" Filtering for version = '${filterVersion.get}'")

      val schema = buildSchema
      val ignoredCount = sc.accumulator(0, "Number of Records Ignored")
      val processedCount = sc.accumulator(0, "Number of Records Processed")

/*
      val messages = Dataset("telemetry")
        .where("sourceName") {
          case "telemetry" => true
        }.where("sourceVersion") {
        case "4" => true
      }.where("docType") {
        case "sync" => true
      }.where("appName") {
        case "Firefox" => true
      }.where("submissionDate") {
        case date if date == currentDate.toString("yyyyMMdd") => true
      }.where("appUpdateChannel") {
        case channel => filterChannel.isEmpty || channel == filterChannel.get
      }.where("appVersion") {
        case v => filterVersion.isEmpty || v == filterVersion.get
      }.records(conf.limit.get)
*/
/*
      val rowRDD = messages.flatMap(m => {
        messageToRow(m) match {
          case None =>
            ignoredCount += 1
            None
          case x =>
            processedCount += 1
            x
        }
      })
*/
/*
      val records = sqlContext.createDataFrame(rowRDD, schema)

      // Note we cannot just use 'partitionBy' below to automatically populate
      // the submission_date partition, because none of the write modes do
      // quite what we want:
      //  - "overwrite" causes the entire vX partition to be deleted and replaced with
      //    the current day's data, so doesn't work with incremental jobs
      //  - "append" would allow us to generate duplicate data for the same day, so
      //    we would need to add some manual checks before running
      //  - "error" (the default) causes the job to fail after any data is
      //    loaded, so we can't do single day incremental updates.
      //  - "ignore" causes new data not to be saved.
      // So we manually add the "submission_date_s3" parameter to the s3path.
      val s3prefix = s"$jobName/$streamVersion/submission_date_s3=$currentDateString"
      val s3path = s"s3://${conf.outputBucket()}/$s3prefix"

      // Repartition the dataframe by sample_id before saving.
      val partitioned = records.repartition(100, records.col("sample_id"))

      // Then write to S3 using the given fields as path name partitions. If any
      // data already exists for the target day, cowardly refuse to run. In
      // that case, go delete the data from S3 and try again.
      partitioned.write.partitionBy("sample_id").mode("error").parquet(s3path)

      // Then remove the _SUCCESS file so we don't break Spark partition discovery.
      S3Store.deleteKey(conf.outputBucket(), s"$s3prefix/_SUCCESS")
*/
      println(s"JOB $jobName COMPLETED SUCCESSFULLY FOR $currentDateString")
      println("     RECORDS SEEN:    %d".format(ignoredCount.value + processedCount.value))
      println("     RECORDS IGNORED: %d".format(ignoredCount.value))
      println("=======================================================================================")
    }
  }

  // Convert the given Heka message containing a "sync" ping
  // to a (list of?) maps containing all the fields.
  // XXX - for now, assume the "old" sync ping - one ping per message...
  def messageToRow(message: Message): Option[Row] = {
    val fields = message.fieldsAsMap()

    // Don't compute the expensive stuff until we need it. We may skip a record
    // due to missing required fields.
    /*lazy*/ val payload = parse(message.payload.getOrElse("{}").asInstanceOf[String])
    //    lazy val application = payload \ "application"
    lazy val build = parse(fields.getOrElse("application.buildId", "{}").asInstanceOf[String])
    SyncPingConverter.payloadToRow(payload \ "payload")
  }

  def buildSchema = SyncPingConverter.pingType
}

object SyncPingConverter {
  val failureType = StructType(List(
    // failures are probably *too* flexible in the schema, but all current errors have a "name" and a second field
    // that is a string or an int. To keep things simple and small here, we just define a string "value" field and
    // convert ints to the string.
    StructField("name", StringType, nullable = false),
    StructField("value", StringType, nullable = true)
  ))

  // XXX - this looks dodgy - I'm sure there's a more scala-ish way to write this...
  def failureReasonToRow(failure: JValue): Option[Row] = failure match {
    case JObject(x) =>
      implicit val formats = DefaultFormats
      Some(Row(
        (failure \ "name").extract[String],
        (failure \ "name").extract[String] match {
          case "httperror" => (failure \ "code").extract[String]
          case "nserror" => (failure \ "code").extract[String]
          case "shutdownerror" => null
          case "autherror" => (failure \ "from").extract[String]
          case "othererror" => (failure \ "error").extract[String]
          case "unexpectederror" => (failure \ "error").extract[String]
          case _ => null
        }
      ))
    case _ =>
      None
  }

  // The record of incoming sync-records.
  val incomingType = StructType(List(
    StructField("applied", LongType, nullable = false),
    StructField("failed", LongType, nullable = false),
    StructField("newFailed", LongType, nullable = false),
    StructField("reconciled", LongType, nullable = false)
  ))
  // Create a row representing incomingType
  def incomingToRow(incoming: JValue): Option[Row] = incoming match {
    case JObject(x) =>
      Some(Row(
        incoming \ "applied" match {
          case JInt(x) => x.toLong
          case _ => 0L
        },
        incoming \ "failed" match {
          case JInt(x) => x.toLong
          case _ => 0L
        },
        incoming \ "newFailed" match {
          case JInt(x) => x.toLong
          case _ => 0L
        },
        incoming \ "reconciled" match {
          case JInt(x) => x.toLong
          case _ => 0L
        }
      ))
    case _ => None
  }

  // Outgoing records.
  val outgoingType = StructType(List(
    StructField("sent", LongType, nullable = false),
    StructField("failed", LongType, nullable = false)
  ))
  def outgoingToRow(outgoing: JValue): Option[List[Row]] = outgoing match {
    case JArray(x) =>
      val buf = scala.collection.mutable.ListBuffer.empty[Row]
      for (outgoing_entry <- x) {
        buf.append(Row(
          outgoing_entry \ "sent" match {
          case JInt(x) => x.toLong
          case _ => 0L
          },
          outgoing_entry \ "failed" match {
            case JInt(x) => x.toLong
            case _ => 0L
          }
        ))
      }
      if (buf.isEmpty) None
      else Some(buf.toList)
    case _ => None
  }

  // The schema for an engine.
  val engineType = StructType(List(
    StructField("name", StringType, nullable = false),
    StructField("took", LongType, nullable = true),
    StructField("status", StringType, nullable = true),
    StructField("failureReason", failureType, nullable = true),
    StructField("incoming", incomingType, nullable = true),
    StructField("outgoing", ArrayType(outgoingType, containsNull = false), nullable = true)
  ))

  // Parse an element of "engines" elt in a ping. Apparently we just return a Row that must match engineType?
  def engineToRow(engine: JValue): Row = {
    Row(
      engine \ "name" match {
        case JString(x) => x
        // XXX - validation - should maybe be Option[Row] -> None?
        case _ => null
      },
      engine \ "took" match {
        case JInt(x) => x.toLong
        case _ => null
      },
      engine \ "status" match {
        case JString(x) => x
        case _ => null
      },
      failureReasonToRow(engine \ "failureReason"),
      incomingToRow(engine \ "incoming"),
      outgoingToRow(engine \ "outgoing")
    )
  }

  def toEnginesRows(engines: JValue): Option[List[Row]] = engines match {
    case JArray(x) =>
      val buf = scala.collection.mutable.ListBuffer.empty[Row]
      // Need simple array iteration??
      for (e <- x) {
        buf.append(engineToRow(e))
      }
      if (buf.isEmpty) None
      else Some(buf.toList)
    case _ => None
  }

  // The status for the Sync itself (ie, not the status for an engine - that's just a string)
  val statusType = StructType(List(
    StructField("sync", StringType, nullable = true),
    StructField("service", StringType, nullable = true)
  ))
  def statusToRow(status: JValue): Option[Row] = status match {
    case JObject(x) =>
      Some(Row(
        status \ "sync" match {
          case JString(x) => x
          case _ => null
        },
        status \ "service" match {
          case JString(x) => x
          case _ => null
        }
      ))
    case _ => None
  }

  // The record of a single sync event.
  def pingType = StructType(List(
    StructField("uid", StringType, nullable = false),
    StructField("when", LongType, nullable = false),
    StructField("took", LongType, nullable = false),
    StructField("version", LongType, nullable = false),
    StructField("failureReason", failureType, nullable = true),
    StructField("status", statusType, nullable = true),
    // "why" is defined in the client-side schema but currently never populated.
    StructField("why", StringType, nullable = true),
    StructField("engines", ArrayType(SyncPingConverter.engineType, containsNull = false), nullable = true)
  ))

  def payloadToRow(payload: JValue): Option[Row] = {
    val row = Row(
      payload \ "uid" match {
        case JString(x) => x
        case _ => return None // a required field.
      },
      payload \ "when" match {
        case JInt(x) => x.toLong
        case _ => return None
      },
      payload \ "took" match {
        case JInt(x) => x.toLong
        case _ => return None
      },
      payload \ "version" match {
        case JInt(x) => x.toLong
        case _ => return None
      },
      failureReasonToRow(payload \ "failureReason"),
      statusToRow(payload \ "status"),
      payload \ "why" match {
        case JString(x) => x
        case _ => null
      },
      toEnginesRows(payload \ "engines")
    )

    Some(row)
  }

}
