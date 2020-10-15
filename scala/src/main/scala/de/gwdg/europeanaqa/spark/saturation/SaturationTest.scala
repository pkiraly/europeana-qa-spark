package de.gwdg.europeanaqa.spark.saturation

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SaturationTest {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SaturationTest")
    val log = org.apache.log4j.LogManager.getLogger("SaturationTest")
    val sc = new SparkContext(conf)

    val dir = args(0);
    val sourceFile = dir + "/" + args(1);
    val outputFile = dir + "/" + args(2);
    val filterType = args(3)
    val i = args(4).toInt
    var range = args(5).toInt
    var first = (i * range) + 1
    var last  = (i * range) + range
    println("from " + first + " to " + last)

    val fieldNames = Array("proxy_dc_title", "proxy_dcterms_alternative", "proxy_dc_description", "proxy_dc_creator", "proxy_dc_publisher", "proxy_dc_contributor", "proxy_dc_type", "proxy_dc_identifier", "proxy_dc_language", "proxy_dc_coverage", "proxy_dcterms_temporal", "proxy_dcterms_spatial", "proxy_dc_subject", "proxy_dc_date", "proxy_dcterms_created", "proxy_dcterms_issued", "proxy_dcterms_extent", "proxy_dcterms_medium", "proxy_dcterms_provenance", "proxy_dcterms_hasPart", "proxy_dcterms_isPartOf", "proxy_dc_format", "proxy_dc_source", "proxy_dc_rights", "proxy_dc_relation", "proxy_edm_europeanaProxy", "proxy_edm_year", "proxy_edm_userTag", "proxy_ore_ProxyIn", "proxy_ore_ProxyFor", "proxy_dcterms_conformsTo", "proxy_dcterms_hasFormat", "proxy_dcterms_hasVersion", "proxy_dcterms_isFormatOf", "proxy_dcterms_isReferencedBy", "proxy_dcterms_isReplacedBy", "proxy_dcterms_isRequiredBy", "proxy_dcterms_isVersionOf", "proxy_dcterms_references", "proxy_dcterms_replaces", "proxy_dcterms_requires", "proxy_dcterms_tableOfContents", "proxy_edm_currentLocation", "proxy_edm_hasMet", "proxy_edm_hasType", "proxy_edm_incorporates", "proxy_edm_isDerivativeOf", "proxy_edm_isRelatedTo", "proxy_edm_isRepresentationOf", "proxy_edm_isSimilarTo", "proxy_edm_isSuccessorOf", "proxy_edm_realizes", "proxy_edm_wasPresentAt", "aggregation_edm_rights", "aggregation_edm_provider", "aggregation_edm_dataProvider", "aggregation_dc_rights", "aggregation_edm_ugc", "aggregation_edm_aggregatedCHO", "aggregation_edm_intermediateProvider", "place_dcterms_isPartOf", "place_dcterms_hasPart", "place_skos_prefLabel", "place_skos_altLabel", "place_skos_note", "agent_edm_begin", "agent_edm_end", "agent_edm_hasMet", "agent_edm_isRelatedTo", "agent_owl_sameAs", "agent_foaf_name", "agent_dc_date", "agent_dc_identifier", "agent_rdaGr2_dateOfBirth", "agent_rdaGr2_placeOfBirth", "agent_rdaGr2_dateOfDeath", "agent_rdaGr2_placeOfDeath", "agent_rdaGr2_dateOfEstablishment", "agent_rdaGr2_dateOfTermination", "agent_rdaGr2_gender", "agent_rdaGr2_professionOrOccupation", "agent_rdaGr2_biographicalInformation", "agent_skos_prefLabel", "agent_skos_altLabel", "agent_skos_note", "timespan_edm_begin", "timespan_edm_end", "timespan_dcterms_isPartOf", "timespan_dcterms_hasPart", "timespan_edm_isNextInSequence", "timespan_owl_sameAs", "timespan_skos_prefLabel", "timespan_skos_altLabel", "timespan_skos_note", "concept_skos_broader", "concept_skos_narrower", "concept_skos_related", "concept_skos_broadMatch", "concept_skos_narrowMatch", "concept_skos_relatedMatch", "concept_skos_exactMatch", "concept_skos_closeMatch", "concept_skos_notation", "concept_skos_inScheme", "concept_skos_prefLabel", "concept_skos_altLabel", "concept_skos_note", "total")

    println("length: " + fieldNames.length)
    val fieldsRDD = sc.parallelize(fieldNames)

    case class Unit(key: String, value: Double)

    def extract: ((Int, Int, Array[Double]) => Seq[Unit]) = {
        (c, d, fieldScores) =>
          fieldScores.zipWithIndex.map{case(value, idx) =>
            var prefix = ""
            if (filterType == "c") {
              prefix = "c" + c + ":"
            } else if (filterType == "d") {
              prefix = "d" + d + ":"
            }
            Unit(prefix + idx, value)
          }
    }

    def calculate: ((String, Iterable[Double]) => (String, Int, Double, Double, Double, Double, Double, Double)) = {
      (fieldParts, numbers) =>
        val parts = fieldParts.split(":")
        val c = parts(0);
        val field = parts(1).toInt
        val count = numbers.count(x => true).toDouble
        val min = numbers.min
        val max = numbers.max
        val range = (numbers.max - numbers.min)
        var mean = 0.0
        var medianVal = 0.0
        var stdDeviationVal = 0.0
        if (range > 0.0) {
          mean = (numbers.sum / count)
          // medianVal = median(count, numbers)
          stdDeviationVal = stdDeviation(count, mean, numbers)
        }
        (
            c, field, min, max, range, mean, medianVal, stdDeviationVal
        )
    }

    def median: ((Double, Iterable[Double]) => Double) = {
        (count, numbers) =>
        val sorted = numbers.toList.sortWith(_ < _)
        if (count % 2 == 0) {
          val l = (count.toInt / 2) - 1
          val r = l + 1
          (sorted(l) + sorted(r)) / 2
        } else {
          sorted(count.toInt / 2)
        }
    }

    def stdDeviation: ((Double, Double, Iterable[Double]) => Double) = {
        (count, mean, numbers) =>
        val sum = (numbers
            .map(num => scala.math.pow((mean - num), 2))
            .reduce(_ + _))
        scala.math.sqrt(sum / count)
    }

    // val sourceFile = "hdfs://localhost:54310/join/result12-language.csv"
    val language = sc.textFile(sourceFile).filter(_.nonEmpty)

    // val count = language.count().toDouble
    val prepared = language
      .map(line => line.split(","))
      .map(line => (line(1).toInt, line(2).toInt, line))

    // prepared.cache()
    val filtered = prepared
        .filter{case(c, d, line) =>
          ((filterType == "c" && first <= c && c <= last) || (filterType == "d" && first <= d && d <= last))
        }
        .map{case(c, d, line) =>
          (c, d, line.slice(3, line.length).map(x => x.toDouble))
        }

      val pairs = filtered
        .map{case(c, d, fields) => (c, d, fields, (fields.reduce(_ + _) / fields.length))}
        .map{case(c, d, fields, total) => (c, d, (fields :+ total))}
        .flatMap{case(c, d, fieldScores) =>
          extract(c, d, fieldScores)
        }
        .map(unit => (unit.key, unit.value))

      val groupped = pairs.groupByKey()

      val calculated = groupped.map{case(field, numbers) =>
          calculate(field, numbers)
        }

      val formatted = calculated
        .sortBy(x => (x._1, x._2))
        .map{
          case(col, field, min, max, range, mean, median, stddev) => (
            col, fieldNames(field), f"$min%.6f", f"$max%.6f", f"$range%.6f", f"$mean%.6f", f"$median%.6f", f"$stddev%.6f"
        )}

      println("saving " + outputFile)
      formatted.
        saveAsTextFile(outputFile)
  }
}
