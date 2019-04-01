package de.gwdg.europeanaqa.spark.languages

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Languages {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Languages")
    val sc = new SparkContext(conf)

    val inputFile = args(0);
    val outputFile = args(1);

    // val sourceFile = "hdfs://localhost:54310/join/result12-language.csv"
    val language = sc.textFile(inputFile).filter(_.nonEmpty)

    val language2 = language.
      map(line => line.
                     replace(",", "','").
                     concat("'").
                     replace("''", "_2:1").
                     split(",").
                     map(elem => elem.replaceAll("'", "")))

    val language3 = language2.flatMap(x => List(
      "proxy_dc_title;" + x(3),
      "proxy_dcterms_alternative;" + x(4),
      "proxy_dc_description;" + x(5),
      "proxy_dc_creator;" + x(6),
      "proxy_dc_publisher;" + x(7),
      "proxy_dc_contributor;" + x(8),
      "proxy_dc_type;" + x(9),
      "proxy_dc_identifier;" + x(10),
      "proxy_dc_language;" + x(11),
      "proxy_dc_coverage;" + x(12),
      "proxy_dcterms_temporal;" + x(13),
      "proxy_dcterms_spatial;" + x(14),
      "proxy_dc_subject;" + x(15),
      "proxy_dc_date;" + x(16),
      "proxy_dcterms_created;" + x(17),
      "proxy_dcterms_issued;" + x(18),
      "proxy_dcterms_extent;" + x(19),
      "proxy_dcterms_medium;" + x(20),
      "proxy_dcterms_provenance;" + x(21),
      "proxy_dcterms_hasPart;" + x(22),
      "proxy_dcterms_isPartOf;" + x(23),
      "proxy_dc_format;" + x(24),
      "proxy_dc_source;" + x(25),
      "proxy_dc_rights;" + x(26),
      "proxy_dc_relation;" + x(27),
      "proxy_edm_year;" + x(28),
      "proxy_edm_userTag;" + x(29),
      "proxy_dcterms_conformsTo;" + x(30),
      "proxy_dcterms_hasFormat;" + x(31),
      "proxy_dcterms_hasVersion;" + x(32),
      "proxy_dcterms_isFormatOf;" + x(33),
      "proxy_dcterms_isReferencedBy;" + x(34),
      "proxy_dcterms_isReplacedBy;" + x(35),
      "proxy_dcterms_isRequiredBy;" + x(36),
      "proxy_dcterms_isVersionOf;" + x(37),
      "proxy_dcterms_references;" + x(38),
      "proxy_dcterms_replaces;" + x(39),
      "proxy_dcterms_requires;" + x(40),
      "proxy_dcterms_tableOfContents;" + x(41),
      "proxy_edm_currentLocation;" + x(42),
      "proxy_edm_hasMet;" + x(43),
      "proxy_edm_hasType;" + x(44),
      "proxy_edm_incorporates;" + x(45),
      "proxy_edm_isDerivativeOf;" + x(46),
      "proxy_edm_isRelatedTo;" + x(47),
      "proxy_edm_isRepresentationOf;" + x(48),
      "proxy_edm_isSimilarTo;" + x(49),
      "proxy_edm_isSuccessorOf;" + x(50),
      "proxy_edm_realizes;" + x(51),
      "proxy_edm_wasPresentAt;" + x(52),
      "aggregation_edm_rights;" + x(53),
      "aggregation_edm_provider;" + x(54),
      "aggregation_edm_dataProvider;" + x(55),
      "aggregation_dc_rights;" + x(56),
      "aggregation_edm_ugc;" + x(57),
      "aggregation_edm_aggregatedCHO;" + x(58),
      "aggregation_edm_intermediateProvider;" + x(59),
      "place_dcterms_isPartOf;" + x(60),
      "place_dcterms_hasPart;" + x(61),
      "place_skos_prefLabel;" + x(62),
      "place_skos_altLabel;" + x(63),
      "place_skos_note;" + x(64),
      "agent_edm_begin;" + x(65),
      "agent_edm_end;" + x(66),
      "agent_edm_hasMet;" + x(67),
      "agent_edm_isRelatedTo;" + x(68),
      "agent_owl_sameAs;" + x(69),
      "agent_foaf_name;" + x(70),
      "agent_dc_date;" + x(71),
      "agent_dc_identifier;" + x(72),
      "agent_rdaGr2_dateOfBirth;" + x(73),
      "agent_rdaGr2_placeOfBirth;" + x(74),
      "agent_rdaGr2_dateOfDeath;" + x(75),
      "agent_rdaGr2_placeOfDeath;" + x(76),
      "agent_rdaGr2_dateOfEstablishment;" + x(77),
      "agent_rdaGr2_dateOfTermination;" + x(78),
      "agent_rdaGr2_gender;" + x(79),
      "agent_rdaGr2_professionOrOccupation;" + x(80),
      "agent_rdaGr2_biographicalInformation;" + x(81),
      "agent_skos_prefLabel;" + x(82),
      "agent_skos_altLabel;" + x(83),
      "agent_skos_note;" + x(84),
      "timespan_edm_begin;" + x(85),
      "timespan_edm_end;" + x(86),
      "timespan_dcterms_isPartOf;" + x(87),
      "timespan_dcterms_hasPart;" + x(88),
      "timespan_edm_isNextInSequence;" + x(89),
      "timespan_owl_sameAs;" + x(90),
      "timespan_skos_prefLabel;" + x(91),
      "timespan_skos_altLabel;" + x(92),
      "timespan_skos_note;" + x(93),
      "concept_skos_broader;" + x(94),
      "concept_skos_narrower;" + x(95),
      "concept_skos_related;" + x(96),
      "concept_skos_broadMatch;" + x(97),
      "concept_skos_narrowMatch;" + x(98),
      "concept_skos_relatedMatch;" + x(99),
      "concept_skos_exactMatch;" + x(100),
      "concept_skos_closeMatch;" + x(101),
      "concept_skos_notation;" + x(102),
      "concept_skos_inScheme;" + x(103),
      "concept_skos_prefLabel;" + x(104),
      "concept_skos_altLabel;" + x(105),
      "concept_skos_note;" + x(106)
    ))                             // -> title;en;de

    val language4 = language3.
      map(x => (x.split(";"))).    // -> (title, en, de)
      map(x => (x.head, x.tail)).  // -> (title, (en, de))
      flatMap(x => x._2.
        map(y => x._1 + "." + y)). // -> (title.en, title.de)
      map(x => (x, 1)).            // -> title.en 1, 1, 1, ....
      reduceByKey(_ + _)          // -> title.en 8

    val language5 = language4.
      map(x => (x._1.split(":"), x._2)).
      map(x => (x._1.head, (Integer.parseInt(x._1.last) * x._2))).
      reduceByKey(_ + _)

    language5.
      map(x => x._1.replace(".", ",") + "," + x._2). // -> "title,en,8"
      saveAsTextFile(outputFile)
  }
}
