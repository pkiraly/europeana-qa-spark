import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame

object SaturationWithHistogramForAll {

  def main(args: Array[String]): Unit = {

    val log = org.apache.log4j.LogManager.getLogger("SaturationStat")
    val spark = SparkSession.builder.appName("SaturationStat").getOrCreate()

    import spark.implicits._

    val inputFile = args(0)
    val outputFile = args(1)

    val dataWithoutHeader = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .format("csv")
      .load(inputFile)

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
      "DistinctLanguagesInObject",
      "TaggedLiteralsPerLanguageInProviderProxy",
      "TaggedLiteralsPerLanguageInEuropeanaProxy",
      "TaggedLiteralsPerLanguageInObject"
    )
    val names = ids ++ individualFields ++ genericFields
    val data = dataWithoutHeader.toDF(names: _*).select(individualFields.map(col): _*)
    data.cache()

    var stat = data
      .select()
      .describe()

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

    var count = data.count()
    var isImpair = count / 2 == 1
    var medianRow = Seq.empty[Any]
    medianRow = medianRow :+ "median"

    for (i <- 0 to (data.schema.fieldNames.size - 1)) {
      var l : Long = -1
      var r : Long = -1
      var median : Double = -1.0
      var fieldName = data.schema.fieldNames(i);

      var histogram = data.select(fieldName)
        .groupBy(fieldName)
        .count()
        .toDF("label", "count")
        .orderBy("label")
        .withColumn("group", functions.lit(1))
        .withColumn("end", sum("count")
          .over(Window.partitionBy("group").orderBy($"label")))
        .withColumn("start", (col("end") - col("count")))

      if (isImpair) {
        l = (count / 2)
        r = l
        median = getMedianFromHistogram(histogram, l)
      } else {
        l = (count / 2) - 1
        r = l + 1
        var lval = getMedianFromHistogram(histogram, l)
        var rval = getMedianFromHistogram(histogram, r)
        median = (lval + rval) / 2
      }

      medianRow = medianRow :+ median
    }

    val labels = Seq("summary") ++ data.schema.fieldNames
    var strmedian = medianRow.map(x => x.toString)
    log.info(labels.size)
    log.info(medianRow.size)
    log.info(strmedian.size)

    var medianDf = Seq((strmedian(0))).toDF(labels(0))
    for (i <- 1 to strmedian.size - 1) {
      medianDf = medianDf.withColumn(labels(i), functions.lit(strmedian(i)))
    }
    stat = stat.union(medianDf)

    /*
    // val medianDf = Seq(strmedian).map(
    //   x => (
    //     x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12)
    //   )
    // ).toDF(labels: _*)
    // stat = stat.union(medianDf)
    */

    stat.write
      .option("header", "true")
      .csv(outputFile) // "hdfs://localhost:54310/join/result29-multilingual-saturation-light-statistics"

    // val medianDf = Seq(strmedian).toDF();
    medianDf.write
      .option("header", "true")
      .csv(outputFile + "-median") // "hdfs://localhost:54310/join/result29-multilingual-saturation-light-statistics"
  }
}

