package de.gwdg.europeanaqa.spark.saturation

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, first, regexp_replace, sum, udf}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import java.io.ByteArrayOutputStream

import org.apache.log4j.{Level, Logger}

object MultilingualityWithHistogramWithMapReduce {

  def main(args: Array[String]): Unit = {

    val log = org.apache.log4j.LogManager.getLogger("MultilingualityWithHistogramWithMapReduce")
    // Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder.appName("MultilingualityWithHistogramWithMapReduce").getOrCreate()
    import spark.implicits._
    val startFields = System.currentTimeMillis()

    val configMap : Map[String, String] = spark.conf.getAll
    for ((key, value) <- configMap) {
      log.info(s"key: $key, value: $value")
    }

    val inputFile = args(0)
    // val outputFile = args(1)

    log.info("reading the data")
    val dataWithoutHeader = spark.read.
      option("header", "false").
      option("inferSchema", "true").
      format("csv").
      load(inputFile)

    log.info("setting names")
    val ids = Seq("id", "c", "d")

    val individualFieldNames = Seq(
      "dc_title", "dcterms_alternative", "dc_description", "dc_creator", "dc_publisher",
      "dc_contributor", "dc_type", "dc_identifier", "dc_language", "dc_coverage",
      "dcterms_temporal", "dcterms_spatial", "dc_subject", "dc_date", "dcterms_created",
      "dcterms_issued", "dcterms_extent", "dcterms_medium", "dcterms_provenance",
      "dcterms_hasPart", "dcterms_isPartOf", "dc_format", "dc_source", "dc_rights",
      "dc_relation", "edm_year", "edm_userTag", "dcterms_conformsTo", "dcterms_hasFormat",
      "dcterms_hasVersion", "dcterms_isFormatOf", "dcterms_isReferencedBy",
      "dcterms_isReplacedBy", "dcterms_isRequiredBy", "dcterms_isVersionOf",
      "dcterms_references", "dcterms_replaces", "dcterms_requires", "dcterms_tableOfContents",
      "edm_currentLocation", "edm_hasMet", "edm_hasType", "edm_incorporates",
      "edm_isDerivativeOf", "edm_isRelatedTo", "edm_isRepresentationOf", "edm_isSimilarTo",
      "edm_isSuccessorOf", "edm_realizes", "edm_wasPresentAt"
    )
    val individualFields = individualFieldNames.
      flatMap(i => Seq(s"provider_$i", s"europeana_$i")).
      flatMap(i => Seq(s"${i}_taggedLiterals", s"${i}_languages", s"${i}_literalsPerLanguage"))

    val genericFields = Seq(
      "NumberOfLanguagesPerPropertyInProviderProxy",
      "NumberOfLanguagesPerPropertyInEuropeanaProxy",
      "NumberOfLanguagesPerPropertyInObject",
      "TaggedLiteralsInProviderProxy",
      "TaggedLiteralsInEuropeanaProxy",
      "DistinctLanguageCountInProviderProxy",
      "DistinctLanguageCountInEuropeanaProxy",
      "TaggedLiteralsInObject",
      "DistinctLanguageCountInObject",
      "TaggedLiteralsPerLanguageInProviderProxy",
      "TaggedLiteralsPerLanguageInEuropeanaProxy",
      "TaggedLiteralsPerLanguageInObject"
    )
    val names = ids ++ individualFields ++ genericFields
    val selectedNames = individualFields ++ genericFields

    val data = dataWithoutHeader.toDF(names: _*) //.select(selectedNames.map(col): _*)
    // data.cache()
    log.info("reading the data: done")
    var simplenames = data.columns.filterNot(x => x == "id" || x == "c" || x == "d")
    var typeMap = data.schema.map(x => (x.name, x.dataType)).toMap
    var fieldIndex = simplenames.zipWithIndex.toMap

    log.info("create flatted")
    var flatted = data.flatMap{row =>
      var c = row.getAs[Int]("c")
      var d = row.getAs[Int]("d")
      var cid = s"c$c"
      var did = s"d$c"
      var cdid = s"cd-$c-$d"

      var seq = new ListBuffer[Tuple3[String, Int, Double]]()
      for (name <- simplenames) {
        var value = if (typeMap(name) == IntegerType) row.getAs[Int](name).toDouble else row.getAs[Double](name)
        var index = fieldIndex(name)
        seq += Tuple3(cid, index, value)
        // seq += Tuple3(did, index, value)
        // seq += Tuple3(cdid, index, value)
      }
      seq
    }.toDF(Seq("id", "field", "value"): _*)

    log.info("create filtered")
    var filtered = flatted.
      filter(x => x.getAs[Double]("value") != -1.0)

    filtered.cache()

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
      // orderBy("id", "field")

    def median(s: Seq[Double]) = {
      val (lower, upper) = s.sortWith(_<_).splitAt(s.size / 2)
      if (s.size % 2 == 0) (lower.last + upper.head) / 2.0 else upper.head
    }

    log.info("create median")
    var mediansRDD = filtered.rdd.groupBy(row => (row(0), row(1))).
      map(x => Row.fromSeq(Seq(x._1._1, x._1._2, median(x._2.map(x => x(2).asInstanceOf[Double]).toSeq))))

    val mediansFields = StructType(List(
      StructField("med_id", StringType, nullable = false),
      StructField("med_field", IntegerType, nullable = false),
      StructField("median", DoubleType, nullable = false)
    ))

    var mediansDF = spark.createDataFrame(mediansRDD, mediansFields)

    val getFieldIndex = udf((s:String) => fieldIndex(s))

    log.info("join all")
    var statisticsAll = statistics.
      join(mediansDF, (statistics.col("id") === mediansDF.col("med_id") && statistics.col("field") === mediansDF.col("med_field")), "inner").
      select("id", "field", "mean", "min", "max", "count", "median").
      // withColumn("fieldIndex", getFieldIndex(col("field"))).
      orderBy("id", "field")

    log.info("save")
    statisticsAll.write.
      option("header", "false").
      mode(SaveMode.Overwrite).
      csv("multilinguality-csv")

    log.info(s"ALL took ${System.currentTimeMillis() - startFields}")
  }
}

