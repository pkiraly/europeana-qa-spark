import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Cardinality {
  def main(args: Array[String]) {

    val log = org.apache.log4j.LogManager.getLogger("europeana-qa.Cardinality")

    val conf = new SparkConf().setAppName("Cardinality")
    val sc = new SparkContext(conf)

    val cardinalityFile = args(1)
    val frequencyFile = args(2)

    // "hdfs://localhost:54310/join/result11.csv"
    val csv = sc.textFile(args(0)).filter(_.nonEmpty);
    val data = csv.map(line => line.split(",").map(elem => elem.trim)) //lines in rows

    val cardinality = data.flatMap(
      x => List(
        "edm_providedcho_about." + x(137),
        "proxy_dc_title." + x(138),
        "proxy_dcterms_alternative." + x(139),
        "proxy_dc_description." + x(140),
        "proxy_dc_creator." + x(141),
        "proxy_dc_publisher." + x(142),
        "proxy_dc_contributor." + x(143),
        "proxy_dc_type." + x(144),
        "proxy_dc_identifier." + x(145),
        "proxy_dc_language." + x(146),
        "proxy_dc_coverage." + x(147),
        "proxy_dcterms_temporal." + x(148),
        "proxy_dcterms_spatial." + x(149),
        "proxy_dc_subject." + x(150),
        "proxy_dc_date." + x(151),
        "proxy_dcterms_created." + x(152),
        "proxy_dcterms_issued." + x(153),
        "proxy_dcterms_extent." + x(154),
        "proxy_dcterms_medium." + x(155),
        "proxy_dcterms_provenance." + x(156),
        "proxy_dcterms_hasPart." + x(157),
        "proxy_dcterms_isPartOf." + x(158),
        "proxy_dc_format." + x(159),
        "proxy_dc_source." + x(160),
        "proxy_dc_rights." + x(161),
        "proxy_dc_relation." + x(162),
        "proxy_edm_isNextInSequence." + x(163),
        "proxy_edm_type." + x(164),
        "proxy_edm_europeanaProxy." + x(165),
        "proxy_edm_year." + x(166),
        "proxy_edm_userTag." + x(167),
        "proxy_ore_ProxyIn." + x(168),
        "proxy_ore_ProxyFor." + x(169),
        "proxy_dc_conformsTo." + x(170),
        "proxy_dcterms_hasFormat." + x(171),
        "proxy_dcterms_hasVersion." + x(172),
        "proxy_dcterms_isFormatOf." + x(173),
        "proxy_dcterms_isReferencedBy." + x(174),
        "proxy_dcterms_isReplacedBy." + x(175),
        "proxy_dcterms_isRequiredBy." + x(176),
        "proxy_dcterms_isVersionOf." + x(177),
        "proxy_dcterms_references." + x(178),
        "proxy_dcterms_replaces." + x(179),
        "proxy_dcterms_requires." + x(180),
        "proxy_dcterms_tableOfContents." + x(181),
        "proxy_edm_currentLocation." + x(182),
        "proxy_edm_hasMet." + x(183),
        "proxy_edm_hasType." + x(184),
        "proxy_edm_incorporates." + x(185),
        "proxy_edm_isDerivativeOf." + x(186),
        "proxy_edm_isRelatedTo." + x(187),
        "proxy_edm_isRepresentationOf." + x(188),
        "proxy_edm_isSimilarTo." + x(189),
        "proxy_edm_isSuccessorOf." + x(190),
        "proxy_edm_realizes." + x(191),
        "proxy_edm_wasPresentAt." + x(192),
        "aggregation_edm_rights." + x(193),
        "aggregation_edm_provider." + x(194),
        "aggregation_edm_dataProvider." + x(195),
        "aggregation_edm_isShownAt." + x(196),
        "aggregation_edm_isShownBy." + x(197),
        "aggregation_edm_object." + x(198),
        "aggregation_edm_hasView." + x(199),
        "aggregation_dc_rights." + x(200),
        "aggregation_edm_ugc." + x(201),
        "aggregation_edm_aggregatedCHO." + x(202),
        "aggregation_edm_intermediateProvider." + x(203),
        "aggregation_rdf_about." + x(204),
        "place_wgs84_lat." + x(205),
        "place_wgs84_long." + x(206),
        "place_wgs84_alt." + x(207),
        "place_dcterms_isPartOf." + x(208),
        "place_wgs84_pos_lat_long." + x(209),
        "place_dcterms_hasPart." + x(210),
        "place_owl_sameAs." + x(211),
        "place_skos_prefLabel." + x(212),
        "place_skos_altLabel." + x(213),
        "place_skos_note." + x(214),
        "place_rdf_about." + x(215),
        "agent_rdf_about." + x(216),
        "agent_edm_begin." + x(217),
        "agent_edm_end." + x(218),
        "agent_edm_hasMet." + x(219),
        "agent_edm_isRelatedTo." + x(220),
        "agent_owl_sameAs." + x(221),
        "agent_foaf_name." + x(222),
        "agent_dc_date." + x(223),
        "agent_dc_identifier." + x(224),
        "agent_rdaGr2_dateOfBirth." + x(225),
        "agent_rdaGr2_placeOfBirth." + x(226),
        "agent_rdaGr2_dateOfDeath." + x(227),
        "agent_rdaGr2_placeOfDeath." + x(228),
        "agent_rdaGr2_dateOfEstablishment." + x(229),
        "agent_rdaGr2_dateOfTermination." + x(230),
        "agent_rdaGr2_gender." + x(231),
        "agent_rdaGr2_professionOrOccupation." + x(232),
        "agent_rdaGr2_biographicalInformation." + x(233),
        "agent_skos_prefLabel." + x(234),
        "agent_skos_altLabel." + x(235),
        "agent_skos_note." + x(236),
        "timespan_rdf_about." + x(237),
        "timespan_edm_begin." + x(238),
        "timespan_edm_end." + x(239),
        "timespan_dcterms_isPartOf." + x(240),
        "timespan_dcterms_hasPart." + x(241),
        "timespan_edm_isNextInSequence." + x(242),
        "timespan_owl_sameAs." + x(243),
        "timespan_skos_prefLabel." + x(244),
        "timespan_skos_altLabel." + x(245),
        "timespan_skos_note." + x(246),
        "concept_rdf_about." + x(247),
        "concept_skos_broader." + x(248),
        "concept_skos_narrower." + x(249),
        "concept_skos_related." + x(250),
        "concept_skos_broadMatch." + x(251),
        "concept_skos_narrowMatch." + x(252),
        "concept_skos_relatedMatch." + x(253),
        "concept_skos_exactMatch." + x(254),
        "concept_skos_closeMatch." + x(255),
        "concept_skos_notation." + x(256),
        "concept_skos_inScheme." + x(257),
        "concept_skos_prefLabel." + x(258),
        "concept_skos_altLabel." + x(259),
        "concept_skos_note." + x(260)
      ))
      .map(x => (x, 1))
      .reduceByKey(_ + _)
      .map(x => (x._1.split("\\."), x._2))

    cardinality.cache()

    val cardinalityMap = cardinality
      .map(x => (x._1.head, (Integer.parseInt(x._1.last) * x._2)))
      .reduceByKey(_ + _)

    // csv
    // "hdfs://localhost:54310/join/cardinality.csv"
    cardinalityMap
      .map(x => x._1 + "," + x._2)
      .saveAsTextFile(cardinalityFile)
    log.info("cardinality saved");

    cardinality
      .map(x => (x._1.head, (x._1.last, x._2)))
      .groupByKey()
      .mapValues(x => x.toMap)
      // select apart zero and non zero cardinalities
      .map{ case(fieldName, cardinalities) => (
        fieldName,
        cardinalities.getOrElse("0", 0),
        cardinalities.filter(unit => unit._1 != "0")
      )}
      // nonZeros is a cardinality map in which each unit is a counter of cardinality
      // such as (2, 4) which means there are 4 records in which the cardinality is 2
      // unit._1 is the cardinality
      // unit._2 is the number of instances
      .map{ case(fieldName, zeros, nonZeros) => (
        fieldName,
        zeros,
        if (nonZeros.count(unit => true) == 0) { // count all
          0
        } else {
          nonZeros.map(unit => unit._2).reduce(_ + _) // sum of all
        }
      )}
      // calculate nonZero count and proportion 
      .map{ case(fieldName, zeros, nonZeros) => (
        fieldName, nonZeros, (nonZeros.toFloat / (zeros + nonZeros))
      )}
      // return final result
      .map{ case(fieldName, count, proportion) => (
        fieldName + "," + count + "," + proportion
      )}
      .saveAsTextFile(frequencyFile) // "hdfs://localhost:54310/join/frequency.csv"
    log.info("frequency saved");
  }
}
