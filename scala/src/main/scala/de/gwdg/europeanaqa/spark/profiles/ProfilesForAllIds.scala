package de.gwdg.europeanaqa.spark.profiles

import java.io.File
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

import scala.collection.mutable.ListBuffer
import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions.collect_list
// import org.apache.spark.sql

// import org.apache.spark.ml.regression.LinearRegression
// import spark.implicits._

object ProfilesForAllIds {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val log = org.apache.log4j.LogManager.getLogger("ProxyBasedCompletenessFromParquet")
  val spark = SparkSession.builder.appName("ProfilesForAllIds").getOrCreate()
  val conf = new SparkConf().setAppName("Profiles")
  val sc = new SparkContext(conf)
  import spark.implicits._

  val longformParquet = "profiles-longform.parquet"
  val patternCountsDir = "profiles-patterns.csv.dir"
  var fieldCountsDir = "profiles-field-counts.csv.dir"
  var fieldIndexDir = "profiles-field-index.csv.dir"
  var dir :File = null;

  def main(args: Array[String]): Unit = {

    val startFields = System.currentTimeMillis()

    // var parquetFile = "hdfs://localhost:54310/join/completeness2.parquet"
    var parquetFile = args(0)
    val phase = args(1)
    log.info(s"runing phase: $phase")

    dir = new File(parquetFile).getAbsoluteFile.getParentFile
    log.info(s"dir: $dir")

    if (phase.equals("prepare")) {
      this.runPrepare(parquetFile)
    } else if (phase.equals("statistics")) {
      this.runStatistics()
    } else {
      log.info(s"unrecognized phase '${phase}'")
    }

    log.info(s"ALL took ${System.currentTimeMillis() - startFields}")
  }

  def runPrepare(inputFile: String): Unit = {
    val df = spark.read.load(inputFile)
    var simplenames = df.columns
        .filterNot(
          x =>
            x == "id" ||
            x == "dataset" ||
            x == "dataProvider" ||
            x == "provider" ||
            x == "country" ||
            x == "language"
        )

    var fieldIndexDF = simplenames.
      zipWithIndex.
      toSeq.
      toDF.
      map(x => (
        x.getAs[String](0).
          replace("PROVIDER_Proxy_", "").
          replace("_", ":"),
        x.getAs[Int](1)
      )).
      toDF(Seq("field", "index"): _*)

    fieldIndexDF.
      repartition(1).
      write.
      option("header", "true").
      mode(SaveMode.Overwrite).
      csv(fieldIndexDir)

    var fieldIndex = simplenames.zipWithIndex.toMap
    var typeMap = df.schema.map(x => (x.name, x.dataType)).toMap

    var len = simplenames.length
    var range = Range(1, (len-1))

    var flatted = df.
      flatMap {
        row =>
          var dataset = row.getAs[Int]("dataset")
          var dataProvider = row.getAs[Int]("dataProvider")
          var provider = row.getAs[Int]("provider")
          var country = row.getAs[Int]("country")
          var language = row.getAs[Int]("language")
          var identifiers = Seq(
            "all",
            s"c-$dataset",
            s"d-$dataProvider",
            s"cd-$dataset-$dataProvider",
            s"cp-$dataset-$provider",
            s"pd-$provider-$dataProvider",
            s"cdp-$dataset-$dataProvider-$provider",
            s"p-$provider",
            s"cn-$country",
            s"l-$language"
          )

          var fields = new ListBuffer[Int]();
          for (name <- simplenames) {
            var value = if (typeMap(name) == IntegerType) row.getAs[Int](name).toDouble else row.getAs[Double](name)
            if (value > 0) {
              fields += fieldIndex(name)
            }
          }
          var value = fields.toList.mkString(",")

          var seq = new ListBuffer[Tuple2[String, String]]()
          for (id <- identifiers) {
            seq += Tuple2(id, value)
          }

          seq
      }
      .toDF(Seq("id", "value"): _*)

    flatted.write.
      mode(SaveMode.Overwrite).
      save(this.getPath(longformParquet))
    log.info("longform has been created")
  }

  def runStatistics(): Unit = {

    val fieldIndexDF = spark.read.
      option("header", "true").
      option("inferSchema", "true").
      format("csv").
      load(this.getPath(fieldIndexDir))

    var fieldMap = fieldIndexDF.
      collect().
      map(x => (x.getAs[Int](1), x.getAs[String](0))).
      toMap

    val df = spark.read.load(this.getPath(longformParquet))

    var patternCounts = df.
      groupByKey(x => x.getAs[String]("id") + "@" + x.getAs[String]("value")).
      count().
      toDF(Seq("value", "count"): _*).
      map(x => (
        x.getAs[String]("value").split("@"),
        x.getAs[Long]("count")
      )).
      map(x => (x._1(0), x._1(1), x._2)).
      toDF(Seq("id", "fields", "count"): _*).
      sort($"id", $"count".desc)

    patternCounts.cache()

    var collectionSizeMap = patternCounts.
      groupBy("id").
      sum("count").
      toDF(Seq("id","count"): _*).
      collect().
      map(x => (x.getAs[String](0), x.getAs[Long](1))).
      toMap

    patternCounts.
      map(x => {
        var fieldInts = x.getAs[String]("fields").split(",");
        var fields = new ListBuffer[String]();
        for (i <- fieldInts) {
          fields += fieldMap.getOrElse(i.toInt, "")
        }
        var id = x.getAs[String]("id")
        var total = collectionSizeMap.getOrElse(id, 0).asInstanceOf[Number].longValue
        var count = x.getAs[Long]("count")
        var percent = count * 100.0 / total

        (
          id,
          fields.mkString(";"),
          fields.length,
          count,
          percent
        )
      }).
      toDF(Seq("id", "fields", "nr-of-fields", "occurence", "percent"): _*).
      write.
      option("header", "true").
      mode(SaveMode.Overwrite).
      csv(this.getPath(patternCountsDir))

    var fieldCounts = patternCounts.
      flatMap{
        x => {
          var id = x.getAs[String]("id")
          var count = x.getAs[Long]("count")
          x.getAs[String]("fields").
            split(",").
            map(y => (id, y.toInt, count))
        }
      }.
      toDF(Seq("id", "field", "count"): _*).
      groupBy("id", "field").
      sum("count").
      toDF(Seq("id", "field", "count"): _*)

    var participatingFields = fieldCounts.
      sort("field").
      map(x => (
        x.getAs[String]("id"),
        fieldMap.getOrElse(x.getAs[Int]("field"), "") + "=" + x.getAs[Long]("count").toString
      )).
      toDF(Seq("id","field"): _*).
      groupBy("id").
      agg(collect_list("field") as "fields").
      map(x => (x.getAs[String]("id"), x.getSeq(1).mkString(","))).
      toDF(Seq("id","fields"): _*)

    participatingFields.
      repartition(1).
      write.
      option("header", "true").
      mode(SaveMode.Overwrite).
      csv(this.getPath(fieldCountsDir))
  }

  def getPath(file: String): String = {
    return new File(dir, file).getPath
  }
}

