package de.gwdg.europeanaqa.spark.completeness

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

import org.apache.log4j._

object CompletenessToMinimalParquet {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val log = org.apache.log4j.LogManager.getLogger("CompletenessToParquet")
    val spark = SparkSession.builder.
      appName("CompletenessToMinimalParquet").
      getOrCreate()
    import spark.implicits._

    val inputFile = args(0)
    val parquetFile = args(1)
    log.info(s"CompletenessToParquet $inputFile -> $parquetFile")

    val dataWithoutHeader = spark.read.
      option("header", "false").
      option("inferSchema", "true").
      format("csv").
      load(inputFile)

    dataWithoutHeader.printSchema()

    val names = Seq(
      "id", "collection", "provider",
      "total", "mandatory", "descriptiveness", "searchability", "contextualization", "identification", "browsing",
      "viewing", "reusability", "multilinguality", "existence_of_ProvidedCHO_rdf_about", "existence_of_Proxy_rdf_about", "existence_of_Proxy_dc_title",
      "existence_of_Proxy_dcterms_alternative", "existence_of_Proxy_dc_description", "existence_of_Proxy_dc_creator", "existence_of_Proxy_dc_publisher",
      "existence_of_Proxy_dc_contributor", "existence_of_Proxy_dc_type", "existence_of_Proxy_dc_identifier", "existence_of_Proxy_dc_language", "existence_of_Proxy_dc_coverage",
      "existence_of_Proxy_dcterms_temporal", "existence_of_Proxy_dcterms_spatial", "existence_of_Proxy_dc_subject", "existence_of_Proxy_dc_date",
      "existence_of_Proxy_dcterms_created", "existence_of_Proxy_dcterms_issued", "existence_of_Proxy_dcterms_extent", "existence_of_Proxy_dcterms_medium",
      "existence_of_Proxy_dcterms_provenance", "existence_of_Proxy_dcterms_hasPart", "existence_of_Proxy_dcterms_isPartOf", "existence_of_Proxy_dc_format",
      "existence_of_Proxy_dc_source", "existence_of_Proxy_dc_rights", "existence_of_Proxy_dc_relation", "existence_of_Proxy_edm_isNextInSequence",
      "existence_of_Proxy_edm_type", "existence_of_Proxy_edm_europeanaProxy", "existence_of_Proxy_edm_year", "existence_of_Proxy_edm_userTag", "existence_of_Proxy_ore_ProxyIn",
      "existence_of_Proxy_ore_ProxyFor", "existence_of_Proxy_dcterms_conformsTo", "existence_of_Proxy_dcterms_hasFormat", "existence_of_Proxy_dcterms_hasVersion",
      "existence_of_Proxy_dcterms_isFormatOf", "existence_of_Proxy_dcterms_isReferencedBy", "existence_of_Proxy_dcterms_isReplacedBy", "existence_of_Proxy_dcterms_isRequiredBy",
      "existence_of_Proxy_dcterms_isVersionOf", "existence_of_Proxy_dcterms_references", "existence_of_Proxy_dcterms_replaces", "existence_of_Proxy_dcterms_requires",
      "existence_of_Proxy_dcterms_tableOfContents", "existence_of_Proxy_edm_currentLocation", "existence_of_Proxy_edm_hasMet", "existence_of_Proxy_edm_hasType",
      "existence_of_Proxy_edm_incorporates", "existence_of_Proxy_edm_isDerivativeOf", "existence_of_Proxy_edm_isRelatedTo", "existence_of_Proxy_edm_isRepresentationOf",
      "existence_of_Proxy_edm_isSimilarTo", "existence_of_Proxy_edm_isSuccessorOf", "existence_of_Proxy_edm_realizes", "existence_of_Proxy_edm_wasPresentAt",
      "existence_of_Aggregation_rdf_about", "existence_of_Aggregation_edm_rights", "existence_of_Aggregation_edm_provider", "existence_of_Aggregation_edm_dataProvider",
      "existence_of_Aggregation_edm_isShownAt", "existence_of_Aggregation_edm_isShownBy", "existence_of_Aggregation_edm_object", "existence_of_Aggregation_edm_hasView",
      "existence_of_Aggregation_dc_rights", "existence_of_Aggregation_edm_ugc", "existence_of_Aggregation_edm_aggregatedCHO", "existence_of_Aggregation_edm_intermediateProvider",
      "existence_of_Place_rdf_about", "existence_of_Place_wgs84_lat", "existence_of_Place_wgs84_long", "existence_of_Place_wgs84_alt", "existence_of_Place_dcterms_isPartOf",
      "existence_of_Place_wgs84_pos_lat_long", "existence_of_Place_dcterms_hasPart", "existence_of_Place_owl_sameAs", "existence_of_Place_skos_prefLabel",
      "existence_of_Place_skos_altLabel", "existence_of_Place_skos_note", "existence_of_Agent_rdf_about", "existence_of_Agent_edm_begin", "existence_of_Agent_edm_end",
      "existence_of_Agent_edm_hasMet", "existence_of_Agent_edm_isRelatedTo", "existence_of_Agent_owl_sameAs", "existence_of_Agent_foaf_name", "existence_of_Agent_dc_date",
      "existence_of_Agent_dc_identifier", "existence_of_Agent_rdaGr2_dateOfBirth", "existence_of_Agent_rdaGr2_placeOfBirth", "existence_of_Agent_rdaGr2_dateOfDeath",
      "existence_of_Agent_rdaGr2_placeOfDeath", "existence_of_Agent_rdaGr2_dateOfEstablishment", "existence_of_Agent_rdaGr2_dateOfTermination", "existence_of_Agent_rdaGr2_gender",
      "existence_of_Agent_rdaGr2_professionOrOccupation", "existence_of_Agent_rdaGr2_biographicalInformation", "existence_of_Agent_skos_prefLabel",
      "existence_of_Agent_skos_altLabel", "existence_of_Agent_skos_note", "existence_of_Timespan_rdf_about", "existence_of_Timespan_edm_begin", "existence_of_Timespan_edm_end",
      "existence_of_Timespan_dcterms_isPartOf", "existence_of_Timespan_dcterms_hasPart", "existence_of_Timespan_edm_isNextInSequence", "existence_of_Timespan_owl_sameAs",
      "existence_of_Timespan_skos_prefLabel", "existence_of_Timespan_skos_altLabel", "existence_of_Timespan_skos_note", "existence_of_Concept_rdf_about",
      "existence_of_Concept_skos_broader", "existence_of_Concept_skos_narrower", "existence_of_Concept_skos_related", "existence_of_Concept_skos_broadMatch",
      "existence_of_Concept_skos_narrowMatch", "existence_of_Concept_skos_relatedMatch", "existence_of_Concept_skos_exactMatch", "existence_of_Concept_skos_closeMatch",
      "existence_of_Concept_skos_notation", "existence_of_Concept_skos_inScheme", "existence_of_Concept_skos_prefLabel", "existence_of_Concept_skos_altLabel",
      "existence_of_Concept_skos_note", "existence_of_crd_ProvidedCHO_rdf_about", "crd_Proxy_rdf_about", "crd_Proxy_dc_title", "crd_Proxy_dcterms_alternative",
      "crd_Proxy_dc_description", "crd_Proxy_dc_creator", "crd_Proxy_dc_publisher", "crd_Proxy_dc_contributor", "crd_Proxy_dc_type", "crd_Proxy_dc_identifier",
      "crd_Proxy_dc_language", "crd_Proxy_dc_coverage", "crd_Proxy_dcterms_temporal", "crd_Proxy_dcterms_spatial", "crd_Proxy_dc_subject", "crd_Proxy_dc_date",
      "crd_Proxy_dcterms_created", "crd_Proxy_dcterms_issued", "crd_Proxy_dcterms_extent", "crd_Proxy_dcterms_medium", "crd_Proxy_dcterms_provenance",
      "crd_Proxy_dcterms_hasPart", "crd_Proxy_dcterms_isPartOf", "crd_Proxy_dc_format", "crd_Proxy_dc_source", "crd_Proxy_dc_rights", "crd_Proxy_dc_relation",
      "crd_Proxy_edm_isNextInSequence", "crd_Proxy_edm_type", "crd_Proxy_edm_europeanaProxy", "crd_Proxy_edm_year", "crd_Proxy_edm_userTag", "crd_Proxy_ore_ProxyIn",
      "crd_Proxy_ore_ProxyFor", "crd_Proxy_dcterms_conformsTo", "crd_Proxy_dcterms_hasFormat", "crd_Proxy_dcterms_hasVersion", "crd_Proxy_dcterms_isFormatOf",
      "crd_Proxy_dcterms_isReferencedBy", "crd_Proxy_dcterms_isReplacedBy", "crd_Proxy_dcterms_isRequiredBy", "crd_Proxy_dcterms_isVersionOf", "crd_Proxy_dcterms_references",
      "crd_Proxy_dcterms_replaces", "crd_Proxy_dcterms_requires", "crd_Proxy_dcterms_tableOfContents", "crd_Proxy_edm_currentLocation", "crd_Proxy_edm_hasMet",
      "crd_Proxy_edm_hasType", "crd_Proxy_edm_incorporates", "crd_Proxy_edm_isDerivativeOf", "crd_Proxy_edm_isRelatedTo", "crd_Proxy_edm_isRepresentationOf",
      "crd_Proxy_edm_isSimilarTo", "crd_Proxy_edm_isSuccessorOf", "crd_Proxy_edm_realizes", "crd_Proxy_edm_wasPresentAt", "crd_Aggregation_rdf_about",
      "crd_Aggregation_edm_rights", "crd_Aggregation_edm_provider", "crd_Aggregation_edm_dataProvider", "crd_Aggregation_edm_isShownAt", "crd_Aggregation_edm_isShownBy",
      "crd_Aggregation_edm_object", "crd_Aggregation_edm_hasView", "crd_Aggregation_dc_rights", "crd_Aggregation_edm_ugc", "crd_Aggregation_edm_aggregatedCHO",
      "crd_Aggregation_edm_intermediateProvider", "crd_Place_rdf_about", "crd_Place_wgs84_lat", "crd_Place_wgs84_long", "crd_Place_wgs84_alt", "crd_Place_dcterms_isPartOf",
      "crd_Place_wgs84_pos_lat_long", "crd_Place_dcterms_hasPart", "crd_Place_owl_sameAs", "crd_Place_skos_prefLabel", "crd_Place_skos_altLabel", "crd_Place_skos_note",
      "crd_Agent_rdf_about", "crd_Agent_edm_begin", "crd_Agent_edm_end", "crd_Agent_edm_hasMet", "crd_Agent_edm_isRelatedTo", "crd_Agent_owl_sameAs", "crd_Agent_foaf_name",
      "crd_Agent_dc_date", "crd_Agent_dc_identifier", "crd_Agent_rdaGr2_dateOfBirth", "crd_Agent_rdaGr2_placeOfBirth", "crd_Agent_rdaGr2_dateOfDeath",
      "crd_Agent_rdaGr2_placeOfDeath", "crd_Agent_rdaGr2_dateOfEstablishment", "crd_Agent_rdaGr2_dateOfTermination", "crd_Agent_rdaGr2_gender",
      "crd_Agent_rdaGr2_professionOrOccupation", "crd_Agent_rdaGr2_biographicalInformation", "crd_Agent_skos_prefLabel", "crd_Agent_skos_altLabel", "crd_Agent_skos_note",
      "crd_Timespan_rdf_about", "crd_Timespan_edm_begin", "crd_Timespan_edm_end", "crd_Timespan_dcterms_isPartOf", "crd_Timespan_dcterms_hasPart",
      "crd_Timespan_edm_isNextInSequence", "crd_Timespan_owl_sameAs", "crd_Timespan_skos_prefLabel", "crd_Timespan_skos_altLabel", "crd_Timespan_skos_note",
      "crd_Concept_rdf_about", "crd_Concept_skos_broader", "crd_Concept_skos_narrower", "crd_Concept_skos_related", "crd_Concept_skos_broadMatch",
      "crd_Concept_skos_narrowMatch", "crd_Concept_skos_relatedMatch", "crd_Concept_skos_exactMatch", "crd_Concept_skos_closeMatch", "crd_Concept_skos_notation",
      "crd_Concept_skos_inScheme", "crd_Concept_skos_prefLabel", "crd_Concept_skos_altLabel", "crd_Concept_skos_note", "long_subject", "same_title_and_description",
      "empty_string")

    val data = dataWithoutHeader.toDF(names: _*)
    // data.printSchema

    // data.select("crd_Proxy_dc_description").describe().show()

    var columns = data.
      columns.
      filter(field =>
        (
          field.equals("provider")
            ||
            (
              field.startsWith("existence_of_Proxy")
                && !field.endsWith("rdf_about")
                && !field.endsWith("ProxyFor")
                && !field.endsWith("ProxyIn")
                && !field.endsWith("europeanaProxy")
                && !field.endsWith("isNextInSequence")
              )
          )
      ).
      map(data(_))
    var existence = data.select(columns: _*)
    var simplenames = existence.columns.map(field =>
      field.replace("existence_of_Proxy_", "")
        // .replace("edm_type", "edmtype")
        .replace("dc_", "dc:")
        .replace("dcterms_", "dcterms:")
        .replace("edm_", "edm:")
        .replace("ore_", "ore:")
    )
    val df = existence.toDF(simplenames: _*)

    df.write.save(parquetFile)
  }
}
