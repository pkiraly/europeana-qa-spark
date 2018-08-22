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
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode

object SaturationFieldContribution {

  def main(args: Array[String]): Unit = {

    val log = org.apache.log4j.LogManager.getLogger("SaturationFieldContribution")
    val spark = SparkSession.builder.appName("SaturationFieldContribution").getOrCreate()
    import spark.implicits._

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
    val individualFields = Seq(
      "provider_dc_title_taggedLiterals", "provider_dc_title_languages", "provider_dc_title_literalsPerLanguage",
      "europeana_dc_title_taggedLiterals", "europeana_dc_title_languages", "europeana_dc_title_literalsPerLanguage",
      "provider_dcterms_alternative_taggedLiterals", "provider_dcterms_alternative_languages", "provider_dcterms_alternative_literalsPerLanguage",
      "europeana_dcterms_alternative_taggedLiterals", "europeana_dcterms_alternative_languages", "europeana_dcterms_alternative_literalsPerLanguage",
      "provider_dc_description_taggedLiterals", "provider_dc_description_languages", "provider_dc_description_literalsPerLanguage",
      "europeana_dc_description_taggedLiterals", "europeana_dc_description_languages", "europeana_dc_description_literalsPerLanguage",
      "provider_dc_creator_taggedLiterals", "provider_dc_creator_languages", "provider_dc_creator_literalsPerLanguage",
      "europeana_dc_creator_taggedLiterals", "europeana_dc_creator_languages", "europeana_dc_creator_literalsPerLanguage",
      "provider_dc_publisher_taggedLiterals", "provider_dc_publisher_languages", "provider_dc_publisher_literalsPerLanguage",
      "europeana_dc_publisher_taggedLiterals", "europeana_dc_publisher_languages", "europeana_dc_publisher_literalsPerLanguage",
      "provider_dc_contributor_taggedLiterals", "provider_dc_contributor_languages", "provider_dc_contributor_literalsPerLanguage",
      "europeana_dc_contributor_taggedLiterals", "europeana_dc_contributor_languages", "europeana_dc_contributor_literalsPerLanguage",
      "provider_dc_type_taggedLiterals", "provider_dc_type_languages", "provider_dc_type_literalsPerLanguage",
      "europeana_dc_type_taggedLiterals", "europeana_dc_type_languages", "europeana_dc_type_literalsPerLanguage",
      "provider_dc_identifier_taggedLiterals", "provider_dc_identifier_languages", "provider_dc_identifier_literalsPerLanguage",
      "europeana_dc_identifier_taggedLiterals", "europeana_dc_identifier_languages", "europeana_dc_identifier_literalsPerLanguage",
      "provider_dc_language_taggedLiterals", "provider_dc_language_languages", "provider_dc_language_literalsPerLanguage",
      "europeana_dc_language_taggedLiterals", "europeana_dc_language_languages", "europeana_dc_language_literalsPerLanguage",
      "provider_dc_coverage_taggedLiterals", "provider_dc_coverage_languages", "provider_dc_coverage_literalsPerLanguage",
      "europeana_dc_coverage_taggedLiterals", "europeana_dc_coverage_languages", "europeana_dc_coverage_literalsPerLanguage",
      "provider_dcterms_temporal_taggedLiterals", "provider_dcterms_temporal_languages", "provider_dcterms_temporal_literalsPerLanguage",
      "europeana_dcterms_temporal_taggedLiterals", "europeana_dcterms_temporal_languages", "europeana_dcterms_temporal_literalsPerLanguage",
      "provider_dcterms_spatial_taggedLiterals", "provider_dcterms_spatial_languages", "provider_dcterms_spatial_literalsPerLanguage",
      "europeana_dcterms_spatial_taggedLiterals", "europeana_dcterms_spatial_languages", "europeana_dcterms_spatial_literalsPerLanguage",
      "provider_dc_subject_taggedLiterals", "provider_dc_subject_languages", "provider_dc_subject_literalsPerLanguage",
      "europeana_dc_subject_taggedLiterals", "europeana_dc_subject_languages", "europeana_dc_subject_literalsPerLanguage",
      "provider_dc_date_taggedLiterals", "provider_dc_date_languages", "provider_dc_date_literalsPerLanguage",
      "europeana_dc_date_taggedLiterals", "europeana_dc_date_languages", "europeana_dc_date_literalsPerLanguage",
      "provider_dcterms_created_taggedLiterals", "provider_dcterms_created_languages", "provider_dcterms_created_literalsPerLanguage",
      "europeana_dcterms_created_taggedLiterals", "europeana_dcterms_created_languages", "europeana_dcterms_created_literalsPerLanguage",
      "provider_dcterms_issued_taggedLiterals", "provider_dcterms_issued_languages", "provider_dcterms_issued_literalsPerLanguage",
      "europeana_dcterms_issued_taggedLiterals", "europeana_dcterms_issued_languages", "europeana_dcterms_issued_literalsPerLanguage",
      "provider_dcterms_extent_taggedLiterals", "provider_dcterms_extent_languages", "provider_dcterms_extent_literalsPerLanguage",
      "europeana_dcterms_extent_taggedLiterals", "europeana_dcterms_extent_languages", "europeana_dcterms_extent_literalsPerLanguage",
      "provider_dcterms_medium_taggedLiterals", "provider_dcterms_medium_languages", "provider_dcterms_medium_literalsPerLanguage",
      "europeana_dcterms_medium_taggedLiterals", "europeana_dcterms_medium_languages", "europeana_dcterms_medium_literalsPerLanguage",
      "provider_dcterms_provenance_taggedLiterals", "provider_dcterms_provenance_languages", "provider_dcterms_provenance_literalsPerLanguage",
      "europeana_dcterms_provenance_taggedLiterals", "europeana_dcterms_provenance_languages", "europeana_dcterms_provenance_literalsPerLanguage",
      "provider_dcterms_hasPart_taggedLiterals", "provider_dcterms_hasPart_languages", "provider_dcterms_hasPart_literalsPerLanguage",
      "europeana_dcterms_hasPart_taggedLiterals", "europeana_dcterms_hasPart_languages", "europeana_dcterms_hasPart_literalsPerLanguage",
      "provider_dcterms_isPartOf_taggedLiterals", "provider_dcterms_isPartOf_languages", "provider_dcterms_isPartOf_literalsPerLanguage",
      "europeana_dcterms_isPartOf_taggedLiterals", "europeana_dcterms_isPartOf_languages", "europeana_dcterms_isPartOf_literalsPerLanguage",
      "provider_dc_format_taggedLiterals", "provider_dc_format_languages", "provider_dc_format_literalsPerLanguage",
      "europeana_dc_format_taggedLiterals", "europeana_dc_format_languages", "europeana_dc_format_literalsPerLanguage",
      "provider_dc_source_taggedLiterals", "provider_dc_source_languages", "provider_dc_source_literalsPerLanguage",
      "europeana_dc_source_taggedLiterals", "europeana_dc_source_languages", "europeana_dc_source_literalsPerLanguage",
      "provider_dc_rights_taggedLiterals", "provider_dc_rights_languages", "provider_dc_rights_literalsPerLanguage",
      "europeana_dc_rights_taggedLiterals", "europeana_dc_rights_languages", "europeana_dc_rights_literalsPerLanguage",
      "provider_dc_relation_taggedLiterals", "provider_dc_relation_languages", "provider_dc_relation_literalsPerLanguage",
      "europeana_dc_relation_taggedLiterals", "europeana_dc_relation_languages", "europeana_dc_relation_literalsPerLanguage",
      "provider_edm_year_taggedLiterals", "provider_edm_year_languages", "provider_edm_year_literalsPerLanguage",
      "europeana_edm_year_taggedLiterals", "europeana_edm_year_languages", "europeana_edm_year_literalsPerLanguage",
      "provider_edm_userTag_taggedLiterals", "provider_edm_userTag_languages", "provider_edm_userTag_literalsPerLanguage",
      "europeana_edm_userTag_taggedLiterals", "europeana_edm_userTag_languages", "europeana_edm_userTag_literalsPerLanguage",
      "provider_dcterms_conformsTo_taggedLiterals", "provider_dcterms_conformsTo_languages", "provider_dcterms_conformsTo_literalsPerLanguage",
      "europeana_dcterms_conformsTo_taggedLiterals", "europeana_dcterms_conformsTo_languages", "europeana_dcterms_conformsTo_literalsPerLanguage",
      "provider_dcterms_hasFormat_taggedLiterals", "provider_dcterms_hasFormat_languages", "provider_dcterms_hasFormat_literalsPerLanguage",
      "europeana_dcterms_hasFormat_taggedLiterals", "europeana_dcterms_hasFormat_languages", "europeana_dcterms_hasFormat_literalsPerLanguage",
      "provider_dcterms_hasVersion_taggedLiterals", "provider_dcterms_hasVersion_languages", "provider_dcterms_hasVersion_literalsPerLanguage",
      "europeana_dcterms_hasVersion_taggedLiterals", "europeana_dcterms_hasVersion_languages", "europeana_dcterms_hasVersion_literalsPerLanguage",
      "provider_dcterms_isFormatOf_taggedLiterals", "provider_dcterms_isFormatOf_languages", "provider_dcterms_isFormatOf_literalsPerLanguage",
      "europeana_dcterms_isFormatOf_taggedLiterals", "europeana_dcterms_isFormatOf_languages", "europeana_dcterms_isFormatOf_literalsPerLanguage",
      "provider_dcterms_isReferencedBy_taggedLiterals", "provider_dcterms_isReferencedBy_languages", "provider_dcterms_isReferencedBy_literalsPerLanguage",
      "europeana_dcterms_isReferencedBy_taggedLiterals", "europeana_dcterms_isReferencedBy_languages", "europeana_dcterms_isReferencedBy_literalsPerLanguage",
      "provider_dcterms_isReplacedBy_taggedLiterals", "provider_dcterms_isReplacedBy_languages", "provider_dcterms_isReplacedBy_literalsPerLanguage",
      "europeana_dcterms_isReplacedBy_taggedLiterals", "europeana_dcterms_isReplacedBy_languages", "europeana_dcterms_isReplacedBy_literalsPerLanguage",
      "provider_dcterms_isRequiredBy_taggedLiterals", "provider_dcterms_isRequiredBy_languages", "provider_dcterms_isRequiredBy_literalsPerLanguage",
      "europeana_dcterms_isRequiredBy_taggedLiterals", "europeana_dcterms_isRequiredBy_languages", "europeana_dcterms_isRequiredBy_literalsPerLanguage",
      "provider_dcterms_isVersionOf_taggedLiterals", "provider_dcterms_isVersionOf_languages", "provider_dcterms_isVersionOf_literalsPerLanguage",
      "europeana_dcterms_isVersionOf_taggedLiterals", "europeana_dcterms_isVersionOf_languages", "europeana_dcterms_isVersionOf_literalsPerLanguage",
      "provider_dcterms_references_taggedLiterals", "provider_dcterms_references_languages", "provider_dcterms_references_literalsPerLanguage",
      "europeana_dcterms_references_taggedLiterals", "europeana_dcterms_references_languages", "europeana_dcterms_references_literalsPerLanguage",
      "provider_dcterms_replaces_taggedLiterals", "provider_dcterms_replaces_languages", "provider_dcterms_replaces_literalsPerLanguage",
      "europeana_dcterms_replaces_taggedLiterals", "europeana_dcterms_replaces_languages", "europeana_dcterms_replaces_literalsPerLanguage",
      "provider_dcterms_requires_taggedLiterals", "provider_dcterms_requires_languages", "provider_dcterms_requires_literalsPerLanguage",
      "europeana_dcterms_requires_taggedLiterals", "europeana_dcterms_requires_languages", "europeana_dcterms_requires_literalsPerLanguage",
      "provider_dcterms_tableOfContents_taggedLiterals", "provider_dcterms_tableOfContents_languages", "provider_dcterms_tableOfContents_literalsPerLanguage",
      "europeana_dcterms_tableOfContents_taggedLiterals", "europeana_dcterms_tableOfContents_languages", "europeana_dcterms_tableOfContents_literalsPerLanguage",
      "provider_edm_currentLocation_taggedLiterals", "provider_edm_currentLocation_languages", "provider_edm_currentLocation_literalsPerLanguage",
      "europeana_edm_currentLocation_taggedLiterals", "europeana_edm_currentLocation_languages", "europeana_edm_currentLocation_literalsPerLanguage",
      "provider_edm_hasMet_taggedLiterals", "provider_edm_hasMet_languages", "provider_edm_hasMet_literalsPerLanguage",
      "europeana_edm_hasMet_taggedLiterals", "europeana_edm_hasMet_languages", "europeana_edm_hasMet_literalsPerLanguage",
      "provider_edm_hasType_taggedLiterals", "provider_edm_hasType_languages", "provider_edm_hasType_literalsPerLanguage",
      "europeana_edm_hasType_taggedLiterals", "europeana_edm_hasType_languages", "europeana_edm_hasType_literalsPerLanguage",
      "provider_edm_incorporates_taggedLiterals", "provider_edm_incorporates_languages", "provider_edm_incorporates_literalsPerLanguage",
      "europeana_edm_incorporates_taggedLiterals", "europeana_edm_incorporates_languages", "europeana_edm_incorporates_literalsPerLanguage",
      "provider_edm_isDerivativeOf_taggedLiterals", "provider_edm_isDerivativeOf_languages", "provider_edm_isDerivativeOf_literalsPerLanguage",
      "europeana_edm_isDerivativeOf_taggedLiterals", "europeana_edm_isDerivativeOf_languages", "europeana_edm_isDerivativeOf_literalsPerLanguage",
      "provider_edm_isRelatedTo_taggedLiterals", "provider_edm_isRelatedTo_languages", "provider_edm_isRelatedTo_literalsPerLanguage",
      "europeana_edm_isRelatedTo_taggedLiterals", "europeana_edm_isRelatedTo_languages", "europeana_edm_isRelatedTo_literalsPerLanguage",
      "provider_edm_isRepresentationOf_taggedLiterals", "provider_edm_isRepresentationOf_languages", "provider_edm_isRepresentationOf_literalsPerLanguage",
      "europeana_edm_isRepresentationOf_taggedLiterals", "europeana_edm_isRepresentationOf_languages", "europeana_edm_isRepresentationOf_literalsPerLanguage",
      "provider_edm_isSimilarTo_taggedLiterals", "provider_edm_isSimilarTo_languages", "provider_edm_isSimilarTo_literalsPerLanguage",
      "europeana_edm_isSimilarTo_taggedLiterals", "europeana_edm_isSimilarTo_languages", "europeana_edm_isSimilarTo_literalsPerLanguage",
      "provider_edm_isSuccessorOf_taggedLiterals", "provider_edm_isSuccessorOf_languages", "provider_edm_isSuccessorOf_literalsPerLanguage",
      "europeana_edm_isSuccessorOf_taggedLiterals", "europeana_edm_isSuccessorOf_languages", "europeana_edm_isSuccessorOf_literalsPerLanguage",
      "provider_edm_realizes_taggedLiterals", "provider_edm_realizes_languages", "provider_edm_realizes_literalsPerLanguage",
      "europeana_edm_realizes_taggedLiterals", "europeana_edm_realizes_languages", "europeana_edm_realizes_literalsPerLanguage",
      "provider_edm_wasPresentAt_taggedLiterals", "provider_edm_wasPresentAt_languages", "provider_edm_wasPresentAt_literalsPerLanguage",
      "europeana_edm_wasPresentAt_taggedLiterals", "europeana_edm_wasPresentAt_languages", "europeana_edm_wasPresentAt_literalsPerLanguage"
    )
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
    val selectedNames = individualFields.filter(x => (x.endsWith("_taggedLiterals")))

    val data = dataWithoutHeader.toDF(names: _*).select(selectedNames.map(col): _*)
    data.cache()
    log.info("reading the data: done")

    data.printSchema()

    val statData = data.map{r =>
      var fieldNames = r.schema.fieldNames
      var prTagged = 0
      var prTotal = 0
      var euTagged = 0
      var euTotal = 0
      for (i <- 0 until r.size) {
        val isPr = fieldNames(i).startsWith("provider_")
        val value = r.getInt(i)
        if (value > -1) {
          if (isPr) {
            prTotal = prTotal + 1
          } else {
            euTotal = euTotal + 1
          }
          if (value > 0) {
            if (isPr)
              prTagged = prTagged + 1
            else
              euTagged = euTagged + 1
          }
        }
      }
      val prPerc = if (prTotal == 0) 0.0 else (prTagged/prTotal)
      val euPerc = if (euTotal == 0) 0.0 else (euTagged/euTotal)
      (prTagged, prTotal, prPerc, euTagged, euTotal, euPerc)
    }.toDF("prTagged", "prTotal", "prPerc", "euTagged", "euTotal", "euPerc")

    statData.agg(max("prTagged"), max("prPerc"), max("euPerc")).show()

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
      var first = histogram.filter($"start" <= l && $"end" >= l)
                           .select("label")
                           .first()
      getDouble(first)
    }

    var total = statData.count()
    var isImpair = total / 2 == 1

    var stat2 = Seq(("fake", "fake", 0.0)).toDF("metric", "field", "value")

    for (i <- 0 to (statData.schema.fieldNames.size - 1)) {
      var l : Long = -1
      var r : Long = -1
      var median : Double = -1.0
      var zerosPerc : Double = -1.0
      var fieldName = statData.schema.fieldNames(i);
      var dataType = statData.schema.fields(i).dataType;
      log.info(s"calculating the median for $fieldName ($dataType)")

      var existing = statData.select(fieldName)
      total = existing.count()
      isImpair = total / 2 == 1
      log.info(s"total: $total")

      if (total > 0) {
        stat2 = stat2.union(toLongForm(existing.describe()))

        var histogram = existing
          .groupBy(fieldName)
          .count()
          .toDF("label", "count")
          .orderBy("label")
          .withColumn("group", functions.lit(1))
          .withColumn("end", sum("count")
            .over(Window.partitionBy("group").orderBy($"label")))
          .withColumn("start", (col("end") - col("count")))

        var lowest = histogram.select("label").first();
        if (dataType.equals(DoubleType))
          log.info("lowest: " + lowest.getDouble(0))
        else
          log.info("lowest: " + lowest.getInt(0))

        var zeros = histogram.select("count").first().getLong(0)
        zerosPerc = zeros * 100.0 / total

        if (isImpair) {
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
      } else {
        stat2 = stat2.union(Seq(
          ("count", fieldName, 0),
          ("mean", fieldName, 0),
          ("stddev", fieldName, 0),
          ("min", fieldName, 0),
          ("max", fieldName, 0)
        ).toDF("metric", "field", "value"))
      }

      log.info(s"$fieldName: $median (zeros: $zerosPerc%)")

      stat2 = stat2.union(Seq(
        ("median", fieldName, median),
        ("zerosPerc", fieldName, zerosPerc)
      ).toDF("metric", "field", "value"))
    }

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

    stat2.repartition(1).write.
      option("header", "true").
      csv(outputFile + "-longform")

    wideDf.repartition(1).write.
      option("header", "true").
      mode(SaveMode.Overwrite).
      csv(outputFile)

    log.info("write wideDf")

  }
}

