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

object MultilingualityToParquet {

  def main(args: Array[String]): Unit = {

    val log = org.apache.log4j.LogManager.getLogger("MultilingualityToParquet")
    // Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder.appName("MultilingualityToParquet").getOrCreate()
    import spark.implicits._
    val start = System.currentTimeMillis()

    val configMap : Map[String, String] = spark.conf.getAll
    for ((key, value) <- configMap) {
      log.info(s"key: $key, value: $value")
    }

    val inputFile = args(0)
    val parquetFile = args(1)

    log.info(s"reading the data from $inputFile to $parquetFile")
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
      "dcterms_issued", "dcterms_extent", "dcterms_medium", "dcterms_provenance", "dcterms_hasPart",
      "dcterms_isPartOf", "dc_format", "dc_source", "dc_rights", "dc_relation",
      "edm_year", "edm_userTag", "dcterms_conformsTo", "dcterms_hasFormat", "dcterms_hasVersion",
      "dcterms_isFormatOf", "dcterms_isReferencedBy", "dcterms_isReplacedBy", "dcterms_isRequiredBy",
        "dcterms_isVersionOf",
      "dcterms_references", "dcterms_replaces", "dcterms_requires", "dcterms_tableOfContents", "edm_currentLocation",
      "edm_hasMet", "edm_hasType", "edm_incorporates", "edm_isDerivativeOf", "edm_isRelatedTo",
      "edm_isRepresentationOf", "edm_isSimilarTo", "edm_isSuccessorOf", "edm_realizes", "edm_wasPresentAt"
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

    val df = dataWithoutHeader.toDF(names: _*).select(names.filter(x => x != "id").map(col): _*)
    // data.cache()
    log.info("reading the data: done")

    log.info(s"save to $parquetFile")
    df.write.
      mode(SaveMode.Overwrite).
      save(parquetFile)

    log.info(s"ALL took ${System.currentTimeMillis() - start}")
  }
}

