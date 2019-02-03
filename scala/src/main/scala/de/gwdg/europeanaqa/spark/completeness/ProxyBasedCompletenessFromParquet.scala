package de.gwdg.europeanaqa.spark.completeness

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, first, regexp_replace, sum, udf}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, ArrayType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import scala.util.control.Breaks.{break, breakable}

import java.io.ByteArrayOutputStream

import org.apache.log4j.{Level, Logger}

import de.gwdg.europeanaqa.spark.common.HistogramHelper._

object ProxyBasedCompletenessFromParquet {

  val log = org.apache.log4j.LogManager.getLogger("ProxyBasedCompletenessFromParquet")
  val spark = SparkSession.builder.appName("ProxyBasedCompletenessFromParquet").getOrCreate()
  import spark.implicits._

  val longformParquet = "completeness-longform.parquet"
  val statisticsParquet = "completeness-statistics.parquet"
  val medianParquet = "completeness-median.parquet"
  val fieldIndexCsv = "completeness-fieldIndex"
  val histogramCsv = "completeness-histogram"
  val histogramRawCsv = "completeness-histogram-raw"
  val statisticsCsv = "completeness-csv"

  def main(args: Array[String]): Unit = {

    // Logger.getLogger("org").setLevel(Level.ERROR)
    val startFields = System.currentTimeMillis()

    val inputFile = args(0)
    val phase = args(1)
    log.info(s"runing phase: $phase")

    if (phase.equals("prepare")) {
      this.runPrepare(inputFile)
    } else if (phase.equals("statistics")) {
      this.runStatistics()
    } else if (phase.equals("median")) {
      this.runMedian()
    } else if (phase.equals("histogram")) {
      this.runHistogram()
    } else if (phase.equals("join")) {
      this.runJoin()
    } else {
      log.info(s"unrecognized phase '${phase}'")
    }

    log.info(s"ALL took ${System.currentTimeMillis() - startFields}")
  }

  def runPrepare(inputFile: String): Unit = {
    log.info("reading the data")
    val data = spark.read.load(inputFile)
    data.printSchema()

    log.info("reading the data: done")
    var simplenames = data.columns
      .filterNot(x => x == "id" ||
                      x == "dataset" ||
                      x == "dataProvider" ||
                      x == "provider" ||
                      x == "country" ||
                      x == "language")
    var typeMap = data.schema.map(x => (x.name, x.dataType)).toMap
    var fieldIndex = simplenames.zipWithIndex.toMap

    simplenames.zipWithIndex.toSeq.toDF("field", "index").
      write.
      option("header", "false").
      mode(SaveMode.Overwrite).
      csv(fieldIndexCsv)

    log.info("create flatted")
    var flatted = data.flatMap { row =>
      var c = row.getAs[Int]("dataset")
      var d = row.getAs[Int]("dataProvider")
      var provider = row.getAs[Int]("provider")
      var country = row.getAs[Int]("country")
      var language = row.getAs[Int]("language")
      var cid = s"c$c"
      var did = s"d$d"
      var cdId = s"cd-$c-$d"
      var pdId = s"pd-$provider-$d"
      var providerId = s"p-$provider"
      var countryId = s"cn-$country"
      var languageId = s"l-$language"

      var seq = new ListBuffer[Tuple3[String, Int, Double]]()
      for (name <- simplenames) {
        var value = if (typeMap(name) == IntegerType) row.getAs[Int](name).toDouble else row.getAs[Double](name)
        if (value != -1.0) {
          var index = fieldIndex(name)
          seq += Tuple3("all", index, value)
          seq += Tuple3(cid, index, value)
          seq += Tuple3(did, index, value)
          seq += Tuple3(cdId, index, value)
          seq += Tuple3(pdId, index, value)
          seq += Tuple3(providerId, index, value)
          seq += Tuple3(countryId, index, value)
          seq += Tuple3(languageId, index, value)
        }
      }
      seq
    }.toDF(Seq("id", "field", "value"): _*)

    flatted.write.
      mode(SaveMode.Overwrite).
      save(longformParquet)
  }

  def runStatistics(): Unit = {
    val filtered = spark.read.load(longformParquet)
    log.info("create statistics")

    var statistics = filtered.
      groupBy("id", "field").
      agg(
        "value" -> "avg",
        "value" -> "min",
        "value" -> "max",
        "value" -> "count"
      ).
      toDF(Seq("id", "field", "mean", "min", "max", "count"): _*)

    statistics.write.
      mode(SaveMode.Overwrite).
      save(statisticsParquet)
  }

  def runMedian(): Unit = {
    val filtered = spark.read.load(longformParquet)
    log.info("create median")

    val histogram = filtered.
      groupBy("id", "field", "value").
      count()

    var groupped = histogram.
      sort("id", "field", "value").
      rdd.
      groupBy(row => (row(0), row(1)))

    groupped.cache()

    case class Counter(value: Double, count: Long)

    def median(histogram: Seq[Counter]): Double = {
      var len = 0.0
      var i = 0
      for (x: Counter <- histogram) {
        len += x.count
      }

      var cumsum = 0.0;
      var middle = Math.round(len / 2)
      var median = -1.0;
      breakable {
        for (x: Counter <- histogram) {
          cumsum += x.count
          if (cumsum >= middle) {
            median = x.value
            break
          }
        }
      }
      median
    }

    var mediansRDD = groupped.map{x =>
      Row.fromSeq(
        Seq(
          x._1._1,
          x._1._2,
          median(
            x._2.map(x => Counter(x.getDouble(2), x.getLong(3))).toSeq
          )
        )
      )
    }

    val mediansFields = StructType(List(
      StructField("id", StringType, nullable = false),
      StructField("field", IntegerType, nullable = false),
      StructField("median", DoubleType, nullable = false)
    ))

    var mediansDF = spark.createDataFrame(mediansRDD, mediansFields)
    mediansDF.write.
      mode(SaveMode.Overwrite).
      save(medianParquet)
  }

  def runHistogram(): Unit = {
    log.info("create histogram")

    val filtered = spark.read.load(longformParquet)

    val histogram = filtered.
      groupBy("id", "field", "value").
      count()

    var groupped = histogram.
      sort("id", "field", "value").
      rdd.
      groupBy(row => (row(0), row(1)))

    val fieldIndexDF = spark.read.
      option("inferSchema", "true").
      format("csv").
      load(fieldIndexCsv)

    var fieldMap = fieldIndexDF.collect.
      map(row => (row.getInt(1), row.getString(0))).
      toMap

    val getFieldName = udf((fieldIndex:Int) => fieldMap(fieldIndex))

    var rawHistogramRDD = groupped.map{x =>
      (
        x._1._1,
        x._1._2,
        x._2.
          map{y =>
            Counter(y.getDouble(2), y.getLong(3).toDouble)
          }.
          toSeq
      )
    }
    rawHistogramRDD.cache()

    var rawHistogramFinalRDD = rawHistogramRDD.map{x =>
      Row.fromSeq(
        Seq(
          x._1,
          x._2,
          x._3.
            map(counter => s"${counter.value}:${counter.count}").
            mkString(";")
        )
      )
    }
    var rawHistogramDF = spark.createDataFrame(rawHistogramFinalRDD, histogramFields).
      sort("id", "field").
      withColumn("name", getFieldName(col("field"))).
      drop("field").
      withColumnRenamed("name", "field").
      select("id", "field", "histogram")

    rawHistogramDF.
      repartition(1).
      write.
      mode(SaveMode.Overwrite).
      csv(histogramRawCsv)

    var histogramRDD = rawHistogramRDD.map{x =>
      Row.fromSeq(
        Seq(
          x._1,
          x._2,
          minifyHistogram(x._3).
            map(unit => s"${unit.from}-${unit.until}:${unit.count}").
            mkString(";")
        )
      )
    }

    histogramRDD.take(10).foreach(println)

    var histogramDF = spark.createDataFrame(histogramRDD, histogramFields).
      sort("id", "field").
      withColumn("name", getFieldName(col("field"))).
      drop("field").
      withColumnRenamed("name", "field").
      select("id", "field", "histogram")

    histogramDF.
      repartition(1).
      write.
      mode(SaveMode.Overwrite).
      csv(histogramCsv)
  }

  def runJoin(): Unit = {
    val statisticsDF = spark.read.load(statisticsParquet)
    val mediansDF = spark.read.load(medianParquet)

    val fieldIndexDF = spark.read.
      option("inferSchema", "true").
      format("csv").
      load(fieldIndexCsv)

    var fieldMap = fieldIndexDF.collect.
      map(row => (row.getInt(1), row.getString(0))).
      toMap

    val getFieldName = udf((fieldIndex:Int) => fieldMap(fieldIndex))

    log.info("join all")
    var statisticsAll = statisticsDF.
      join(mediansDF, Seq("id", "field"), "inner").
      select("id", "field", "mean", "min", "max", "count", "median").
      orderBy("id", "field").
      withColumn("name", getFieldName(col("field"))).
      drop("field").
      withColumnRenamed("name", "field").
      select("id", "field", "mean", "min", "max", "count", "median")

    log.info("save")
    statisticsAll.
      repartition(1).
      write.
      option("header", "false").
      mode(SaveMode.Overwrite).
      csv(statisticsCsv)
  }
}

