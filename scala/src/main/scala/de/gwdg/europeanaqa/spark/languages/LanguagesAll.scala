package de.gwdg.europeanaqa.spark.languages

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, udf}
import java.text.SimpleDateFormat

import scala.collection.mutable.ListBuffer

object LanguagesAll {
  val log = org.apache.log4j.LogManager.getLogger("LanguagesAll")
  val spark = SparkSession.builder.appName("LanguagesAll").getOrCreate()
  import spark.implicits._

  val sdf = new SimpleDateFormat("HH:mm:ss.SSS")
  val fieldIndexCsv = "limbo/languages/fieldIndex.csv"
  val longformParquet = "limbo/languages/longform.parquet"

  def main(args: Array[String]) {
    val start = System.currentTimeMillis()

    val inputFile = args(0);
    val outputFile = args(1);
    val phase = args(2)
    log.info(s"inputFile: $inputFile")
    log.info(s"outputFile: $outputFile")
    log.info(s"runing phase: $phase")

    this.runPrepare(inputFile, outputFile)

    /*
    if (phase.equals("prepare")) {
      this.runPrepare(inputFile, outputFile)
    } else if (phase.equals("statistics")) {
      this.runStatistics(outputFile)
    }
    */
    val duration = sdf.format(System.currentTimeMillis() - start - (60*60*1000))
    log.info(s"$phase took $duration")
  }

  def runPrepare(inputFile: String, outputFile: String): Unit = {
    log.info("reading the data")

    val fromParquet = false
    val headerOption = if (fromParquet) "true" else "false"
    val formatOption = if (fromParquet) "parquet" else "csv"

    val dataWithoutHeader = spark.read.
      option("header", headerOption).
      option("inferSchema", "true").
      format(formatOption).
      load(inputFile)

    val ids = Seq("id", "dataset", "dataProvider", "provider", "country", "language")

    val languageFields = Seq(
      "proxy_dc_title", "proxy_dcterms_alternative", "proxy_dc_description",
      "proxy_dc_creator", "proxy_dc_publisher", "proxy_dc_contributor", "proxy_dc_type",
      "proxy_dc_identifier", "proxy_dc_language", "proxy_dc_coverage",
      "proxy_dcterms_temporal", "proxy_dcterms_spatial", "proxy_dc_subject",
      "proxy_dc_date", "proxy_dcterms_created", "proxy_dcterms_issued",
      "proxy_dcterms_extent", "proxy_dcterms_medium", "proxy_dcterms_provenance",
      "proxy_dcterms_hasPart", "proxy_dcterms_isPartOf", "proxy_dc_format",
      "proxy_dc_source", "proxy_dc_rights", "proxy_dc_relation",
      "proxy_dcterms_conformsTo", "proxy_dcterms_hasFormat", "proxy_dcterms_hasVersion",
      "proxy_dcterms_isFormatOf", "proxy_dcterms_isReferencedBy",
      "proxy_dcterms_isReplacedBy", "proxy_dcterms_isRequiredBy",
      "proxy_dcterms_isVersionOf", "proxy_dcterms_references", "proxy_dcterms_replaces",
      "proxy_dcterms_requires", "proxy_dcterms_tableOfContents",
      "proxy_edm_currentLocation", "proxy_edm_hasType", "proxy_edm_isRelatedTo",
      "aggregation_edm_provider", "aggregation_edm_dataProvider",
      "aggregation_dc_rights", "aggregation_edm_intermediateProvider",
      "place_dcterms_isPartOf", "place_dcterms_hasPart", "place_skos_prefLabel",
      "place_skos_altLabel", "place_skos_note",
      "agent_edm_begin", "agent_edm_end", "agent_foaf_name", "agent_dc_date",
      "agent_dc_identifier", "agent_rdaGr2_dateOfBirth", "agent_rdaGr2_placeOfBirth",
      "agent_rdaGr2_dateOfDeath", "agent_rdaGr2_placeOfDeath",
      "agent_rdaGr2_dateOfEstablishment", "agent_rdaGr2_dateOfTermination",
      "agent_rdaGr2_gender", "agent_rdaGr2_professionOrOccupation",
      "agent_rdaGr2_biographicalInformation", "agent_skos_prefLabel",
      "agent_skos_altLabel", "agent_skos_note",
      "timespan_edm_begin", "timespan_edm_end", "timespan_dcterms_isPartOf",
      "timespan_dcterms_hasPart", "timespan_edm_isNextInSequence",
      "timespan_skos_prefLabel", "timespan_skos_altLabel", "timespan_skos_note",
      "concept_skos_prefLabel", "concept_skos_altLabel", "concept_skos_note"
    )

    val names = ids ++ languageFields
    val selectedNames = languageFields

    val data = dataWithoutHeader.toDF(names: _*).
      select(
        names.
          filterNot(_ == "id").
          map(col): _*
      )

    var typeMap = data.schema.map(x => (x.name, x.dataType)).toMap
    var fieldIndex = selectedNames.zipWithIndex.toMap
    var wholeRecordIndex = 1000

    selectedNames.zipWithIndex.toSeq.toDF("field", "index").
      write.
      option("header", "false").
      mode(SaveMode.Overwrite).
      csv(fieldIndexCsv)

    var longForm = data.flatMap { row =>
      var dataset = row.getAs[Int]("dataset")
      var dataProvider = row.getAs[Int]("dataProvider")
      var provider = row.getAs[Int]("provider")
      var country = row.getAs[Int]("country")
      var language = row.getAs[Int]("language")

      // composite identifiers
      var cid = s"c-$dataset"
      var did = s"d-$dataProvider"
      var cdId = s"cd-$dataset-$dataProvider"
      var cpId = s"cp-$dataset-$provider"
      var pdId = s"pd-$provider-$dataProvider"
      var cdpId = s"cdp-$dataset-$dataProvider-$provider"
      var providerId = s"p-$provider"
      var countryId = s"cn-$country"
      var languageId = s"l-$language"

      var ids = Seq("all", cid, did, cdId, cpId, pdId, cdpId, providerId, countryId, languageId)
      var seq = new ListBuffer[Tuple5[String, Int, String, Int, Int]]()

      for (name <- selectedNames) {
        var value = row.getAs[String](name)

        var units = value.split(';')
        for (unit <- units) {
          val languageAndCount = unit.split(":");
          val language = languageAndCount(0);
          val count = Integer.parseInt(languageAndCount(1));

          if (count != -1.0) {
            var index = fieldIndex(name)
            for (id <- ids) {
              seq += Tuple5(id, index, language, count, 1)
              seq += Tuple5(id, wholeRecordIndex, language, count, 1)
            }
          }
        }
      }
      seq
    }.toDF(Seq("id", "field", "language", "occurrence", "record"): _*)

    /*
    longForm.write.
      mode(SaveMode.Overwrite).
      save(longformParquet)
    log.info("preparation ended")
  }

  def runStatistics(outputFile: String): Unit = {

    log.info("create statistics")
    val longForm = spark.read.load(longformParquet)
    */

    val fieldIndexDF = spark.read.
      option("inferSchema", "true").
      format("csv").
      load(fieldIndexCsv)

    var summary = longForm.
      groupBy("id", "field", "language").
      sum("occurrence", "record").
      toDF(Seq("id", "field", "language", "occurrence", "record"): _*)

    var fieldMap = fieldIndexDF.collect.
      map(x => (x.getAs[Int](1), x.getAs[String](0))).
      toMap ++ Seq((1000, "all")).map(x => (x._1, x._2)).toMap

    val getFieldName = udf((index:Int) => fieldMap(index))

    var result = summary.
      orderBy("id", "field").
      withColumn("name", getFieldName(col("field"))).
      drop("field").
      withColumnRenamed("name", "field").
      select("id", "field", "language", "occurrence", "record")

    log.info("save")
    result.
      repartition(1).
      write.
      option("header", "false").
      mode(SaveMode.Overwrite).
      csv(outputFile)
  }
}
