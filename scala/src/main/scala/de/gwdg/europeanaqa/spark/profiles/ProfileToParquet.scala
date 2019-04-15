package de.gwdg.europeanaqa.spark.profiles

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{sum, col, first, regexp_replace}
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{SparkSession, Row, DataFrame, SaveMode}

object ProfileToParquet {

  def main(args: Array[String]): Unit = {

    val log = org.apache.log4j.LogManager.getLogger("ProfileToParquet")
    val spark = SparkSession.builder.appName("ProfileToParquet").getOrCreate()
    import spark.implicits._

    val configMap : Map[String, String] = spark.conf.getAll
    for ((key, value) <- configMap) {
      log.info(s"key: $key, value: $value")
    }

    val inputFile = args(0)
    val outputFile = args(1)
    val fromParquetRaw = args(2)
    val fromParquet = fromParquetRaw.equals("from-parquet")
    val headerOption = fromParquet
    val formatOption = if (fromParquet) "parquet" else "csv"

    log.info(s"ProfileToParquet $inputFile -> $outputFile")

    log.info(s"reading the data from $inputFile")
    val dataWithoutHeader = spark.read.
      option("header", headerOption).
      option("inferSchema", "true").
      format(formatOption).
      load(inputFile)

    if (!fromParquet) {
      log.info("setting names")
      val ids = Seq("id", "dataset", "dataProvider", "provider", "country", "language")
      val cardinalityFields = Seq(
        "PROVIDER_Proxy_rdf_about", "PROVIDER_Proxy_dc_title", "PROVIDER_Proxy_dcterms_alternative",
        "PROVIDER_Proxy_dc_description", "PROVIDER_Proxy_dc_creator", "PROVIDER_Proxy_dc_publisher",
        "PROVIDER_Proxy_dc_contributor", "PROVIDER_Proxy_dc_type", "PROVIDER_Proxy_dc_identifier",
        "PROVIDER_Proxy_dc_language", "PROVIDER_Proxy_dc_coverage", "PROVIDER_Proxy_dcterms_temporal",
        "PROVIDER_Proxy_dcterms_spatial", "PROVIDER_Proxy_dc_subject", "PROVIDER_Proxy_dc_date",
        "PROVIDER_Proxy_dcterms_created", "PROVIDER_Proxy_dcterms_issued", "PROVIDER_Proxy_dcterms_extent",
        "PROVIDER_Proxy_dcterms_medium", "PROVIDER_Proxy_dcterms_provenance", "PROVIDER_Proxy_dcterms_hasPart",
        "PROVIDER_Proxy_dcterms_isPartOf", "PROVIDER_Proxy_dc_format", "PROVIDER_Proxy_dc_source",
        "PROVIDER_Proxy_dc_rights", "PROVIDER_Proxy_dc_relation", "PROVIDER_Proxy_edm_isNextInSequence",
        "PROVIDER_Proxy_edm_type", "PROVIDER_Proxy_edm_europeanaProxy", "PROVIDER_Proxy_edm_year",
        "PROVIDER_Proxy_edm_userTag", "PROVIDER_Proxy_ore_proxyIn", "PROVIDER_Proxy_ore_proxyFor",
        "PROVIDER_Proxy_dcterms_conformsTo", "PROVIDER_Proxy_dcterms_hasFormat", "PROVIDER_Proxy_dcterms_hasVersion",
        "PROVIDER_Proxy_dcterms_isFormatOf", "PROVIDER_Proxy_dcterms_isReferencedBy",
        "PROVIDER_Proxy_dcterms_isReplacedBy", "PROVIDER_Proxy_dcterms_isRequiredBy",
        "PROVIDER_Proxy_dcterms_isVersionOf", "PROVIDER_Proxy_dcterms_references", "PROVIDER_Proxy_dcterms_replaces",
        "PROVIDER_Proxy_dcterms_requires", "PROVIDER_Proxy_dcterms_tableOfContents",
        "PROVIDER_Proxy_edm_currentLocation", "PROVIDER_Proxy_edm_hasMet", "PROVIDER_Proxy_edm_hasType",
        "PROVIDER_Proxy_edm_incorporates", "PROVIDER_Proxy_edm_isDerivativeOf", "PROVIDER_Proxy_edm_isRelatedTo",
        "PROVIDER_Proxy_edm_isRepresentationOf", "PROVIDER_Proxy_edm_isSimilarTo",
        "PROVIDER_Proxy_edm_isSuccessorOf", "PROVIDER_Proxy_edm_realizes", "PROVIDER_Proxy_edm_wasPresentAt",

        "PROVIDER_Agent_rdf_about", "PROVIDER_Agent_edm_begin", "PROVIDER_Agent_edm_end", "PROVIDER_Agent_edm_hasMet",
        "PROVIDER_Agent_edm_isRelatedTo", "PROVIDER_Agent_owl_sameAs", "PROVIDER_Agent_foaf_name",
        "PROVIDER_Agent_dc_date", "PROVIDER_Agent_dc_identifier", "PROVIDER_Agent_rdaGr2_dateOfBirth",
        "PROVIDER_Agent_rdaGr2_placeOfBirth", "PROVIDER_Agent_rdaGr2_dateOfDeath",
        "PROVIDER_Agent_rdaGr2_placeOfDeath", "PROVIDER_Agent_rdaGr2_dateOfEstablishment",
        "PROVIDER_Agent_rdaGr2_dateOfTermination", "PROVIDER_Agent_rdaGr2_gender",
        "PROVIDER_Agent_rdaGr2_professionOrOccupation", "PROVIDER_Agent_rdaGr2_biographicalInformation",
        "PROVIDER_Agent_skos_prefLabel", "PROVIDER_Agent_skos_altLabel", "PROVIDER_Agent_skos_note",

        "PROVIDER_Concept_rdf_about", "PROVIDER_Concept_skos_broader", "PROVIDER_Concept_skos_narrower",
        "PROVIDER_Concept_skos_related", "PROVIDER_Concept_skos_broadMatch", "PROVIDER_Concept_skos_narrowMatch",
        "PROVIDER_Concept_skos_relatedMatch", "PROVIDER_Concept_skos_exactMatch", "PROVIDER_Concept_skos_closeMatch",
        "PROVIDER_Concept_skos_notation", "PROVIDER_Concept_skos_inScheme", "PROVIDER_Concept_skos_prefLabel",
        "PROVIDER_Concept_skos_altLabel", "PROVIDER_Concept_skos_note",

        "PROVIDER_Place_rdf_about", "PROVIDER_Place_wgs84_lat", "PROVIDER_Place_wgs84_long",
        "PROVIDER_Place_wgs84_alt", "PROVIDER_Place_dcterms_isPartOf", "PROVIDER_Place_wgs84_pos_lat_long",
        "PROVIDER_Place_dcterms_hasPart", "PROVIDER_Place_owl_sameAs", "PROVIDER_Place_skos_prefLabel",
        "PROVIDER_Place_skos_altLabel", "PROVIDER_Place_skos_note",

        "PROVIDER_Timespan_rdf_about", "PROVIDER_Timespan_edm_begin", "PROVIDER_Timespan_edm_end",
        "PROVIDER_Timespan_dcterms_isPartOf", "PROVIDER_Timespan_dcterms_hasPart",
        "PROVIDER_Timespan_edm_isNextInSequence", "PROVIDER_Timespan_owl_sameAs",
        "PROVIDER_Timespan_skos_prefLabel", "PROVIDER_Timespan_skos_altLabel", "PROVIDER_Timespan_skos_note",

        "EUROPEANA_Proxy_rdf_about", "EUROPEANA_Proxy_dc_title", "EUROPEANA_Proxy_dcterms_alternative",
        "EUROPEANA_Proxy_dc_description", "EUROPEANA_Proxy_dc_creator", "EUROPEANA_Proxy_dc_publisher",
        "EUROPEANA_Proxy_dc_contributor", "EUROPEANA_Proxy_dc_type", "EUROPEANA_Proxy_dc_identifier",
        "EUROPEANA_Proxy_dc_language", "EUROPEANA_Proxy_dc_coverage", "EUROPEANA_Proxy_dcterms_temporal",
        "EUROPEANA_Proxy_dcterms_spatial", "EUROPEANA_Proxy_dc_subject", "EUROPEANA_Proxy_dc_date",
        "EUROPEANA_Proxy_dcterms_created", "EUROPEANA_Proxy_dcterms_issued", "EUROPEANA_Proxy_dcterms_extent",
        "EUROPEANA_Proxy_dcterms_medium", "EUROPEANA_Proxy_dcterms_provenance", "EUROPEANA_Proxy_dcterms_hasPart",
        "EUROPEANA_Proxy_dcterms_isPartOf", "EUROPEANA_Proxy_dc_format", "EUROPEANA_Proxy_dc_source",
        "EUROPEANA_Proxy_dc_rights", "EUROPEANA_Proxy_dc_relation", "EUROPEANA_Proxy_edm_isNextInSequence",
        "EUROPEANA_Proxy_edm_type", "EUROPEANA_Proxy_edm_europeanaProxy", "EUROPEANA_Proxy_edm_year",
        "EUROPEANA_Proxy_edm_userTag", "EUROPEANA_Proxy_ore_proxyIn", "EUROPEANA_Proxy_ore_proxyFor",
        "EUROPEANA_Proxy_dcterms_conformsTo", "EUROPEANA_Proxy_dcterms_hasFormat",
        "EUROPEANA_Proxy_dcterms_hasVersion", "EUROPEANA_Proxy_dcterms_isFormatOf",
        "EUROPEANA_Proxy_dcterms_isReferencedBy", "EUROPEANA_Proxy_dcterms_isReplacedBy",
        "EUROPEANA_Proxy_dcterms_isRequiredBy", "EUROPEANA_Proxy_dcterms_isVersionOf",
        "EUROPEANA_Proxy_dcterms_references", "EUROPEANA_Proxy_dcterms_replaces",
        "EUROPEANA_Proxy_dcterms_requires", "EUROPEANA_Proxy_dcterms_tableOfContents",
        "EUROPEANA_Proxy_edm_currentLocation", "EUROPEANA_Proxy_edm_hasMet", "EUROPEANA_Proxy_edm_hasType",
        "EUROPEANA_Proxy_edm_incorporates", "EUROPEANA_Proxy_edm_isDerivativeOf",
        "EUROPEANA_Proxy_edm_isRelatedTo", "EUROPEANA_Proxy_edm_isRepresentationOf",
        "EUROPEANA_Proxy_edm_isSimilarTo", "EUROPEANA_Proxy_edm_isSuccessorOf", "EUROPEANA_Proxy_edm_realizes",
        "EUROPEANA_Proxy_edm_wasPresentAt",

        "EUROPEANA_Agent_rdf_about", "EUROPEANA_Agent_edm_begin", "EUROPEANA_Agent_edm_end",
        "EUROPEANA_Agent_edm_hasMet", "EUROPEANA_Agent_edm_isRelatedTo", "EUROPEANA_Agent_owl_sameAs",
        "EUROPEANA_Agent_foaf_name", "EUROPEANA_Agent_dc_date", "EUROPEANA_Agent_dc_identifier",
        "EUROPEANA_Agent_rdaGr2_dateOfBirth", "EUROPEANA_Agent_rdaGr2_placeOfBirth",
        "EUROPEANA_Agent_rdaGr2_dateOfDeath", "EUROPEANA_Agent_rdaGr2_placeOfDeath",
        "EUROPEANA_Agent_rdaGr2_dateOfEstablishment", "EUROPEANA_Agent_rdaGr2_dateOfTermination",
        "EUROPEANA_Agent_rdaGr2_gender", "EUROPEANA_Agent_rdaGr2_professionOrOccupation",
        "EUROPEANA_Agent_rdaGr2_biographicalInformation", "EUROPEANA_Agent_skos_prefLabel",
        "EUROPEANA_Agent_skos_altLabel", "EUROPEANA_Agent_skos_note",

        "EUROPEANA_Concept_rdf_about", "EUROPEANA_Concept_skos_broader", "EUROPEANA_Concept_skos_narrower",
        "EUROPEANA_Concept_skos_related", "EUROPEANA_Concept_skos_broadMatch", "EUROPEANA_Concept_skos_narrowMatch",
        "EUROPEANA_Concept_skos_relatedMatch", "EUROPEANA_Concept_skos_exactMatch", "EUROPEANA_Concept_skos_closeMatch",
        "EUROPEANA_Concept_skos_notation", "EUROPEANA_Concept_skos_inScheme", "EUROPEANA_Concept_skos_prefLabel",
        "EUROPEANA_Concept_skos_altLabel", "EUROPEANA_Concept_skos_note",

        "EUROPEANA_Place_rdf_about", "EUROPEANA_Place_wgs84_lat", "EUROPEANA_Place_wgs84_long",
        "EUROPEANA_Place_wgs84_alt", "EUROPEANA_Place_dcterms_isPartOf", "EUROPEANA_Place_wgs84_pos_lat_long",
        "EUROPEANA_Place_dcterms_hasPart", "EUROPEANA_Place_owl_sameAs", "EUROPEANA_Place_skos_prefLabel",
        "EUROPEANA_Place_skos_altLabel", "EUROPEANA_Place_skos_note",

        "EUROPEANA_Timespan_rdf_about", "EUROPEANA_Timespan_edm_begin", "EUROPEANA_Timespan_edm_end",
        "EUROPEANA_Timespan_dcterms_isPartOf", "EUROPEANA_Timespan_dcterms_hasPart",
        "EUROPEANA_Timespan_edm_isNextInSequence", "EUROPEANA_Timespan_owl_sameAs",
        "EUROPEANA_Timespan_skos_prefLabel", "EUROPEANA_Timespan_skos_altLabel",
        "EUROPEANA_Timespan_skos_note"
      )

      val names = ids ++ cardinalityFields
      val selectedNames = cardinalityFields

      val data = dataWithoutHeader.toDF(names: _*)
    } else {
      val data = dataWithoutHeader
    }

    val selectedColumns = dataWithoutHeader.
      columns.
      filter(field => (
        field == "dataset"
          || field == "dataProvider"
          || field == "provider"
          || field == "country"
          || field == "language")
        || field.startsWith("PROVIDER_Proxy_"
      )).
      filterNot(field => (
        field.endsWith("rdf_about")
          || field.endsWith("ProxyFor")
          || field.endsWith("ProxyIn")
          || field.endsWith("europeanaProxy")
          || field.endsWith("isNextInSequence")
        ))

    val selectedData = dataWithoutHeader.select(selectedColumns.map(col): _*)

    selectedData.write.
      option("header", "true").
      mode(SaveMode.Overwrite).
      parquet(outputFile)
  }
}
