package de.gwdg.europeanaqa.spark.saturation

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.first
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import scala.collection.JavaConverters._
import java.io.ByteArrayOutputStream

object SaturationWithHistogramForAll {

  def main(args: Array[String]): Unit = {

    val log = org.apache.log4j.LogManager.getLogger("SaturationWithHistogramForAll")
    val spark = SparkSession.builder.appName("SaturationWithHistogramForAll").getOrCreate()
    import spark.implicits._
    // import sqlContext.implicits._
    // val sqlContext = new SQLContext(spark.sparkContext)
    // import sqlContext.implicits._

    val configMap : Map[String, String] = spark.conf.getAll
    for ((key, value) <- configMap) {
      log.info(s"key: $key, value: $value")
    }

    val inputFile = args(0)
    val outputFile = args(1)

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

    val data = dataWithoutHeader.toDF(names: _*).select(selectedNames.map(col): _*)
    data.cache()
    log.info("reading the data: done")

    data.printSchema()

    def toLongForm(df: DataFrame): DataFrame = {
      val schema = df.schema
      df.flatMap(row => {
        val metric = row.getString(0)
        (1 until row.size).map(i => {
          (metric, schema.fieldNames(i), row.getString(i).toDouble)
        })
      }).toDF("metric", "field", "value")
    }

    def getDouble(first: Row): Double = {
      if (first.schema.fields(0).dataType.equals(DoubleType)) {
        first.getDouble(0)
      } else {
        first.getInt(0).toDouble
      }
    }

    def getMedianFromHistogram(histogram: DataFrame, l: Long): Double = {
      var start = System.currentTimeMillis();
      var first = histogram.filter($"start" <= l && $"end" >= l)
                           .select("label")
                           .first()
      var median = getDouble(first)
      log.info(s"- getMedianFromHistogram took ${System.currentTimeMillis() - start}")

      return median
    }

    def calculateMedian(histogram: DataFrame, isUneven: Boolean, total: Long): Double = {
      var l : Long = -1
      var r : Long = -1
      var median : Double = -1.0

      if (isUneven) {
        l = (total / 2)
        r = l
        median = getMedianFromHistogram(histogram, l)
      } else {
        l = (total / 2) - 1
        r = l + 1
        var lval = getMedianFromHistogram(histogram, l)
        var rval = getMedianFromHistogram(histogram, r)
        median = (lval + rval) / 2
      }
      return median
    }

    def createHistogram(input: DataFrame, fieldName: String): DataFrame = {
      input
        .groupBy(fieldName)
        .count()
        .toDF("label", "count")
        .orderBy("label")
        .withColumn("group", functions.lit(1))
        .withColumn("end", sum("count")
          .over(Window.partitionBy("group").orderBy($"label")))
        .withColumn("start", (col("end") - col("count") + 1))
    }

    var total = data.count()
    var isImpair = total / 2 == 1

    var stat2 = Seq(("fake", "fake", 0.0)).toDF("metric", "field", "value")

    // val tls = data.schema.fieldNames.filter(x => (x.startsWith("provider_") && x.endsWith("_taggedLiterals")))
    // data.schema.fieldNames.filter(startsWith("provider_") && endsWith("_taggedLiterals"))
    // data.select()
    // provider_xxxx_taggedLiterals
    // europeana_xxxx_taggedLiterals

    var startFields = System.currentTimeMillis();
    for (i <- 0 to (data.schema.fieldNames.size - 1)) {
      var startField = System.currentTimeMillis();
      var l : Long = -1
      var r : Long = -1
      var median : Double = -1.0
      var zerosPerc : Double = -1.0
      var fieldName = data.schema.fieldNames(i);
      var dataType = data.schema.fields(i).dataType;
      log.info(s"calculating the median for $fieldName ($dataType)")

      var filterField = fieldName
      if (filterField.endsWith("_languages"))
        filterField = filterField.replace("_languages", "_taggedLiterals")
      else if (filterField.endsWith("_literalsPerLanguage"))
        filterField = filterField.replace("_literalsPerLanguage", "_taggedLiterals")
      log.info(s"- filterField: $filterField")

      var start = System.currentTimeMillis();
      var filteredValues = data.filter(col(filterField) > -1).select(fieldName).collect().toList.asJava
      val aStruct = new StructType(Array(StructField(fieldName, dataType, nullable = true)))
      var existing = spark.createDataFrame(filteredValues, aStruct)
      log.info(s"- existing took ${System.currentTimeMillis() - start}")

      start = System.currentTimeMillis();
      existing.cache()
      total = existing.count()
      var isUneven = (total == 1) || ((total / 2) == 1)
      log.info(s"- total: $total, took ${System.currentTimeMillis() - start}")

      if (total > 0) {
        start = System.currentTimeMillis();
        stat2 = stat2.union(toLongForm(existing.describe()))
        log.info(s"- describe took ${System.currentTimeMillis() - start}")

        start = System.currentTimeMillis();
        var histogram = createHistogram(existing, fieldName)
        histogram.cache()
        log.info(s"- histogram: ${histogram.count()}")
        /*
        var outCapture = new ByteArrayOutputStream
        Console.withOut(outCapture) {
          histogram.show()
        }
        var histogramAsString = new String(outCapture.toByteArray)
        log.info(histogramAsString)
        */
        log.info(s"- histogram took ${System.currentTimeMillis() - start}")

        start = System.currentTimeMillis();
        median = calculateMedian(histogram, isUneven, total)
        log.info(s"- median took ${System.currentTimeMillis() - start}")

        start = System.currentTimeMillis();
        var zeros = histogram.select("count").first().getLong(0)
        zerosPerc = zeros * 100.0 / total
        log.info(s"- zerosPerc took ${System.currentTimeMillis() - start}")

      } else {
        stat2 = stat2.union(Seq(
          ("count", fieldName, 0),
          ("mean", fieldName, 0),
          ("stddev", fieldName, 0),
          ("min", fieldName, 0),
          ("max", fieldName, 0)
        ).toDF("metric", "field", "value"))
      }

      log.info(s"- median: $median (zeros: $zerosPerc%)")

      start = System.currentTimeMillis();
      stat2 = stat2.union(Seq(
        ("median", fieldName, median),
        ("zerosPerc", fieldName, zerosPerc)
      ).toDF("metric", "field", "value"))
      log.info(s"- union took ${System.currentTimeMillis() - start}")

      log.info(s"- took ${System.currentTimeMillis() - startField}")
    }

    log.info("write wideDf")

    var start = System.currentTimeMillis();
    val wideDf = stat2.
      filter(col("field") =!= "fake").
      groupBy("field").
      pivot("metric", Seq("count", "median", "zerosPerc", "mean", "stddev", "min", "max")).
      agg(first("value")).
      withColumn("source", regexp_replace(regexp_replace($"field", "europeana_.*", "b"), "provider_.*", "a")).
      withColumn("type", regexp_replace(regexp_replace(regexp_replace($"field", ".*_taggedLiterals$", "a"), ".*_languages", "b"), ".*_literalsPerLanguage", "c")).
      withColumn("main", regexp_replace($"field", "^(provider|europeana)_(.*)_(taggedLiterals|languages|literalsPerLanguage)$", "$2")).
      orderBy("main", "source", "type").
      select("field", "count", "median", "zerosPerc", "mean", "stddev", "min", "max")
    log.info(s"Creating wideDf took ${System.currentTimeMillis() - start}")

    start = System.currentTimeMillis();
    stat2.repartition(1).write.
      option("header", "true").
      mode(SaveMode.Overwrite).
      csv(s"$outputFile-longform")
    log.info(s"Saving longDF took ${System.currentTimeMillis() - start}")

    start = System.currentTimeMillis();
    wideDf.repartition(1).write.
      option("header", "true").
      mode(SaveMode.Overwrite).
      csv(s"$outputFile-csv")
    log.info(s"Saving wideDf took ${System.currentTimeMillis() - start}")

    /*
    start = System.currentTimeMillis();
    spark.sparkContext.parallelize(List(wideDf.schema.fieldNames.mkString(","))).
      repartition(1).
      toDF().
      write.
      mode(SaveMode.Overwrite).
      format("text").
      save(s"$outputFile-header")
    log.info(s"- union took ${System.currentTimeMillis() - start}")
    */

    log.info(s"ALL took ${System.currentTimeMillis() - startFields}")
  }
}

