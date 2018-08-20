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

object CompletenessToParquet {

  def main(args: Array[String]): Unit = {

    val log = org.apache.log4j.LogManager.getLogger("CompletenessToParquet")
    val spark = SparkSession.builder.appName("CompletenessToParquet").getOrCreate()
    import spark.implicits._

    val configMap : Map[String, String] = spark.conf.getAll
    for ((key, value) <- configMap) {
      log.info(s"key: $key, value: $value")
    }

    val inputFile = args(0)
    val outputFile = args(1)
    val fromParquetRaw = args(2)
    val fromParquet = fromParquetRaw.equals("from-parquet")
    val headerOption = if (fromParquet) "true" else "false"
    val formatOption = if (fromParquet) "parquet" else "csv"

    log.info("reading the data")
    val dataWithoutHeader = spark.read.
      option("header", headerOption).
      option("inferSchema", "true").
      format(formatOption).
      load(inputFile)

    log.info("setting names")
    val ids = Seq("id", "c", "d")

    val completeness = Seq(
      "total", "mandatory", "descriptiveness", "searchability", "contextualization",
      "identification", "browsing", "viewing", "reusability", "multilinguality"
    )

    val instanceFields = Seq(
      "ProvidedCHO_rdf_about",

      "Proxy_rdf_about", "Proxy_dc_title", "Proxy_dcterms_alternative", "Proxy_dc_description", "Proxy_dc_creator",
      "Proxy_dc_publisher", "Proxy_dc_contributor", "Proxy_dc_type", "Proxy_dc_identifier", "Proxy_dc_language",
      "Proxy_dc_coverage", "Proxy_dcterms_temporal", "Proxy_dcterms_spatial", "Proxy_dc_subject", "Proxy_dc_date",
      "Proxy_dcterms_created", "Proxy_dcterms_issued", "Proxy_dcterms_extent", "Proxy_dcterms_medium", "Proxy_dcterms_provenance",
      "Proxy_dcterms_hasPart", "Proxy_dcterms_isPartOf", "Proxy_dc_format", "Proxy_dc_source", "Proxy_dc_rights",
      "Proxy_dc_relation", "Proxy_edm_isNextInSequence", "Proxy_edm_type", "Proxy_edm_europeanaProxy",
      "Proxy_edm_year", "Proxy_edm_userTag", "Proxy_ore_ProxyIn", "Proxy_ore_ProxyFor", "Proxy_dcterms_conformsTo",
      "Proxy_dcterms_hasFormat", "Proxy_dcterms_hasVersion", "Proxy_dcterms_isFormatOf", "Proxy_dcterms_isReferencedBy",
      "Proxy_dcterms_isReplacedBy", "Proxy_dcterms_isRequiredBy", "Proxy_dcterms_isVersionOf", "Proxy_dcterms_references",
      "Proxy_dcterms_replaces", "Proxy_dcterms_requires", "Proxy_dcterms_tableOfContents", "Proxy_edm_currentLocation",
      "Proxy_edm_hasMet", "Proxy_edm_hasType", "Proxy_edm_incorporates", "Proxy_edm_isDerivativeOf", "Proxy_edm_isRelatedTo",
      "Proxy_edm_isRepresentationOf", "Proxy_edm_isSimilarTo", "Proxy_edm_isSuccessorOf", "Proxy_edm_realizes",
      "Proxy_edm_wasPresentAt",

      "Aggregation_rdf_about", "Aggregation_edm_rights", "Aggregation_edm_provider", "Aggregation_edm_dataProvider",
      "Aggregation_edm_isShownAt", "Aggregation_edm_isShownBy", "Aggregation_edm_object", "Aggregation_edm_hasView",
      "Aggregation_dc_rights", "Aggregation_edm_ugc", "Aggregation_edm_aggregatedCHO", "Aggregation_edm_intermediateProvider",

      "Place_rdf_about", "Place_wgs84_lat", "Place_wgs84_long", "Place_wgs84_alt", "Place_dcterms_isPartOf", "Place_wgs84_pos_lat_long",
      "Place_dcterms_hasPart", "Place_owl_sameAs", "Place_skos_prefLabel", "Place_skos_altLabel", "Place_skos_note",

      "Agent_rdf_about", "Agent_edm_begin", "Agent_edm_end", "Agent_edm_hasMet", "Agent_edm_isRelatedTo", "Agent_owl_sameAs",
      "Agent_foaf_name", "Agent_dc_date", "Agent_dc_identifier", "Agent_rdaGr2_dateOfBirth", "Agent_rdaGr2_placeOfBirth",
      "Agent_rdaGr2_dateOfDeath", "Agent_rdaGr2_placeOfDeath", "Agent_rdaGr2_dateOfEstablishment",
      "Agent_rdaGr2_dateOfTermination", "Agent_rdaGr2_gender", "Agent_rdaGr2_professionOrOccupation",
      "Agent_rdaGr2_biographicalInformation", "Agent_skos_prefLabel", "Agent_skos_altLabel", "Agent_skos_note",

      "Timespan_rdf_about", "Timespan_edm_begin", "Timespan_edm_end", "Timespan_dcterms_isPartOf", "Timespan_dcterms_hasPart",
      "Timespan_edm_isNextInSequence", "Timespan_owl_sameAs", "Timespan_skos_prefLabel", "Timespan_skos_altLabel",
      "Timespan_skos_note",

      "Concept_rdf_about", "Concept_skos_broader", "Concept_skos_narrower", "Concept_skos_related", "Concept_skos_broadMatch",
      "Concept_skos_narrowMatch", "Concept_skos_relatedMatch", "Concept_skos_exactMatch", "Concept_skos_closeMatch",
      "Concept_skos_notation", "Concept_skos_inScheme", "Concept_skos_prefLabel", "Concept_skos_altLabel", "Concept_skos_note"
    )

    val cardinalityFields = Seq(
      "crd_ProvidedCHO_rdf_about",

      "crd_Proxy_rdf_about", "crd_Proxy_dc_title", "crd_Proxy_dcterms_alternative", "crd_Proxy_dc_description",
      "crd_Proxy_dc_creator", "crd_Proxy_dc_publisher", "crd_Proxy_dc_contributor", "crd_Proxy_dc_type", "crd_Proxy_dc_identifier",
      "crd_Proxy_dc_language", "crd_Proxy_dc_coverage", "crd_Proxy_dcterms_temporal", "crd_Proxy_dcterms_spatial",
      "crd_Proxy_dc_subject", "crd_Proxy_dc_date", "crd_Proxy_dcterms_created", "crd_Proxy_dcterms_issued",
      "crd_Proxy_dcterms_extent", "crd_Proxy_dcterms_medium", "crd_Proxy_dcterms_provenance", "crd_Proxy_dcterms_hasPart",
      "crd_Proxy_dcterms_isPartOf", "crd_Proxy_dc_format", "crd_Proxy_dc_source", "crd_Proxy_dc_rights", "crd_Proxy_dc_relation",
      "crd_Proxy_edm_isNextInSequence", "crd_Proxy_edm_type", "crd_Proxy_edm_europeanaProxy", "crd_Proxy_edm_year",
      "crd_Proxy_edm_userTag", "crd_Proxy_ore_ProxyIn", "crd_Proxy_ore_ProxyFor", "crd_Proxy_dcterms_conformsTo",
      "crd_Proxy_dcterms_hasFormat", "crd_Proxy_dcterms_hasVersion", "crd_Proxy_dcterms_isFormatOf",
      "crd_Proxy_dcterms_isReferencedBy", "crd_Proxy_dcterms_isReplacedBy", "crd_Proxy_dcterms_isRequiredBy",
      "crd_Proxy_dcterms_isVersionOf", "crd_Proxy_dcterms_references", "crd_Proxy_dcterms_replaces",
      "crd_Proxy_dcterms_requires", "crd_Proxy_dcterms_tableOfContents", "crd_Proxy_edm_currentLocation", "crd_Proxy_edm_hasMet",
      "crd_Proxy_edm_hasType", "crd_Proxy_edm_incorporates", "crd_Proxy_edm_isDerivativeOf", "crd_Proxy_edm_isRelatedTo",
      "crd_Proxy_edm_isRepresentationOf", "crd_Proxy_edm_isSimilarTo", "crd_Proxy_edm_isSuccessorOf", "crd_Proxy_edm_realizes",
      "crd_Proxy_edm_wasPresentAt",

      "crd_Aggregation_rdf_about", "crd_Aggregation_edm_rights", "crd_Aggregation_edm_provider", "crd_Aggregation_edm_dataProvider",
      "crd_Aggregation_edm_isShownAt", "crd_Aggregation_edm_isShownBy", "crd_Aggregation_edm_object",
      "crd_Aggregation_edm_hasView", "crd_Aggregation_dc_rights", "crd_Aggregation_edm_ugc", "crd_Aggregation_edm_aggregatedCHO",
      "crd_Aggregation_edm_intermediateProvider",

      "crd_Place_rdf_about", "crd_Place_wgs84_lat", "crd_Place_wgs84_long", "crd_Place_wgs84_alt", "crd_Place_dcterms_isPartOf",
      "crd_Place_wgs84_pos_lat_long", "crd_Place_dcterms_hasPart", "crd_Place_owl_sameAs", "crd_Place_skos_prefLabel",
      "crd_Place_skos_altLabel", "crd_Place_skos_note",

      "crd_Agent_rdf_about", "crd_Agent_edm_begin", "crd_Agent_edm_end", "crd_Agent_edm_hasMet", "crd_Agent_edm_isRelatedTo",
      "crd_Agent_owl_sameAs", "crd_Agent_foaf_name", "crd_Agent_dc_date", "crd_Agent_dc_identifier",
      "crd_Agent_rdaGr2_dateOfBirth", "crd_Agent_rdaGr2_placeOfBirth", "crd_Agent_rdaGr2_dateOfDeath",
      "crd_Agent_rdaGr2_placeOfDeath", "crd_Agent_rdaGr2_dateOfEstablishment", "crd_Agent_rdaGr2_dateOfTermination",
      "crd_Agent_rdaGr2_gender", "crd_Agent_rdaGr2_professionOrOccupation", "crd_Agent_rdaGr2_biographicalInformation",
      "crd_Agent_skos_prefLabel", "crd_Agent_skos_altLabel", "crd_Agent_skos_note",

      "crd_Timespan_rdf_about", "crd_Timespan_edm_begin", "crd_Timespan_edm_end", "crd_Timespan_dcterms_isPartOf",
      "crd_Timespan_dcterms_hasPart", "crd_Timespan_edm_isNextInSequence", "crd_Timespan_owl_sameAs",
      "crd_Timespan_skos_prefLabel", "crd_Timespan_skos_altLabel", "crd_Timespan_skos_note",

      "crd_Concept_rdf_about", "crd_Concept_skos_broader", "crd_Concept_skos_narrower", "crd_Concept_skos_related",
      "crd_Concept_skos_broadMatch", "crd_Concept_skos_narrowMatch", "crd_Concept_skos_relatedMatch",
      "crd_Concept_skos_exactMatch", "crd_Concept_skos_closeMatch", "crd_Concept_skos_notation", "crd_Concept_skos_inScheme",
      "crd_Concept_skos_prefLabel", "crd_Concept_skos_altLabel", "crd_Concept_skos_note"
    )

    val problemFields = Seq(
      "long_subject", "same_title_and_description", "empty_string"
    )

    val names = ids ++ completeness ++ instanceFields ++ cardinalityFields ++ problemFields
    val selectedNames = completeness ++ instanceFields ++ cardinalityFields ++ problemFields

    val data = dataWithoutHeader.toDF(names: _*).select(names.filterNot(_ == "id").map(col): _*)
    log.info("reading the data: done")

    data.write.
      option("header", "true").
      mode(SaveMode.Overwrite).
      parquet(outputFile)
  }
}
