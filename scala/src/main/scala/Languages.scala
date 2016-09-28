import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Languages {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Languages")
    val sc = new SparkContext(conf)

    val dir = args(0);
    val sourceFile = dir + "/" + args(1);

    // val sourceFile = "hdfs://localhost:54310/join/result12-language.csv"
    val language = sc.textFile(sourceFile).filter(_.nonEmpty)

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
      "proxy_edm_europeanaProxy;" + x(28),
      "proxy_edm_year;" + x(29),
      "proxy_edm_userTag;" + x(30),
      "proxy_ore_ProxyIn;" + x(31),
      "proxy_ore_ProxyFor;" + x(32),
      "proxy_dc_conformsTo;" + x(33),
      "proxy_dcterms_hasFormat;" + x(34),
      "proxy_dcterms_hasVersion;" + x(35),
      "proxy_dcterms_isFormatOf;" + x(36),
      "proxy_dcterms_isReferencedBy;" + x(37),
      "proxy_dcterms_isReplacedBy;" + x(38),
      "proxy_dcterms_isRequiredBy;" + x(39),
      "proxy_dcterms_isVersionOf;" + x(40),
      "proxy_dcterms_references;" + x(41),
      "proxy_dcterms_replaces;" + x(42),
      "proxy_dcterms_requires;" + x(43),
      "proxy_dcterms_tableOfContents;" + x(44),
      "proxy_edm_currentLocation;" + x(45),
      "proxy_edm_hasMet;" + x(46),
      "proxy_edm_hasType;" + x(47),
      "proxy_edm_incorporates;" + x(48),
      "proxy_edm_isDerivativeOf;" + x(49),
      "proxy_edm_isRelatedTo;" + x(50),
      "proxy_edm_isRepresentationOf;" + x(51),
      "proxy_edm_isSimilarTo;" + x(52),
      "proxy_edm_isSuccessorOf;" + x(53),
      "proxy_edm_realizes;" + x(54),
      "proxy_edm_wasPresentAt;" + x(55),
      "aggregation_edm_rights;" + x(56),
      "aggregation_edm_provider;" + x(57),
      "aggregation_edm_dataProvider;" + x(58),
      "aggregation_dc_rights;" + x(59),
      "aggregation_edm_ugc;" + x(60),
      "aggregation_edm_aggregatedCHO;" + x(61),
      "aggregation_edm_intermediateProvider;" + x(62),
      "place_dcterms_isPartOf;" + x(63),
      "place_dcterms_hasPart;" + x(64),
      "place_skos_prefLabel;" + x(65),
      "place_skos_altLabel;" + x(66),
      "place_skos_note;" + x(67),
      "agent_edm_begin;" + x(68),
      "agent_edm_end;" + x(69),
      "agent_edm_hasMet;" + x(70),
      "agent_edm_isRelatedTo;" + x(71),
      "agent_owl_sameAs;" + x(72),
      "agent_foaf_name;" + x(73),
      "agent_dc_date;" + x(74),
      "agent_dc_identifier;" + x(75),
      "agent_rdaGr2_dateOfBirth;" + x(76),
      "agent_rdaGr2_placeOfBirth;" + x(77),
      "agent_rdaGr2_dateOfDeath;" + x(78),
      "agent_rdaGr2_placeOfDeath;" + x(79),
      "agent_rdaGr2_dateOfEstablishment;" + x(80),
      "agent_rdaGr2_dateOfTermination;" + x(81),
      "agent_rdaGr2_gender;" + x(82),
      "agent_rdaGr2_professionOrOccupation;" + x(83),
      "agent_rdaGr2_biographicalInformation;" + x(84),
      "agent_skos_prefLabel;" + x(85),
      "agent_skos_altLabel;" + x(86),
      "agent_skos_note;" + x(87),
      "timespan_edm_begin;" + x(88),
      "timespan_edm_end;" + x(89),
      "timespan_dcterms_isPartOf;" + x(90),
      "timespan_dcterms_hasPart;" + x(91),
      "timespan_edm_isNextInSequence;" + x(92),
      "timespan_owl_sameAs;" + x(93),
      "timespan_skos_prefLabel;" + x(94),
      "timespan_skos_altLabel;" + x(95),
      "timespan_skos_note;" + x(96),
      "concept_skos_broader;" + x(97),
      "concept_skos_narrower;" + x(98),
      "concept_skos_related;" + x(99),
      "concept_skos_broadMatch;" + x(100),
      "concept_skos_narrowMatch;" + x(101),
      "concept_skos_relatedMatch;" + x(102),
      "concept_skos_exactMatch;" + x(103),
      "concept_skos_closeMatch;" + x(104),
      "concept_skos_notation;" + x(105),
      "concept_skos_inScheme;" + x(106),
      "concept_skos_prefLabel;" + x(107),
      "concept_skos_altLabel;" + x(108),
      "concept_skos_note;" + x(109)
    ))                             // -> title;en;de

    val language4 = language3.
      map(x => (x.split(";"))).    // -> (title, en, de)
      map(x => (x.head, x.tail)).  // -> (title, (en, de))
      flatMap(x => x._2.
        map(y => x._1 + "." + y)). // -> (title.en, title.de)
      map(x => (x, 1)).            // -> title.en 1, 1, 1, ....
      reduceByKey(_ + _)          // -> title.en 8

    /*
    val language4 = language3.
      map(x => (x.split(";"))).
      map(x => (x.head, x.tail)).
      flatMap(x => x._2.
        map(y => x._1 + "." + y)).
      map(x => x.split(":")).
      map(x => (x.head, x.last)).
      reduceByKey(_ + _)
    */

    val language5 = language4.
      map(x => (x._1.split(":"), x._2)).
      map(x => (x._1.head, (Integer.parseInt(x._1.last) * x._2))).
      reduceByKey(_ + _)

    language5.
      map(x => x._1.replace(".", ",") + "," + x._2). // -> "title,en,8"
      saveAsTextFile(dir + "/languages.csv")

    /*
    language4
      .map(x => (x._1.split("\\."), x._2))      // -> ((title, en), 8)
      .map(x => (x._1.head, (x._1.last, x._2))) // -> (title, (en, 8))
      .groupByKey()                             // -> (title, ((en, 8), (de, 5), ...))
      .saveAsTextFile(dir + "/languages-groupped.txt")
    */
  }
}
