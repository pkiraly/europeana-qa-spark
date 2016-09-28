import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object LanguagesPerDataProviders {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("LanguagesPerDataProviders")
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
      "c" + x(1) + "-proxy_dc_title;" + x(3),
      "d" + x(2) + "-proxy_dc_title;" + x(3),
      "c" + x(1) + "-proxy_dcterms_alternative;" + x(4),
      "d" + x(2) + "-proxy_dcterms_alternative;" + x(4),
      "c" + x(1) + "-proxy_dc_description;" + x(5),
      "d" + x(2) + "-proxy_dc_description;" + x(5),
      "c" + x(1) + "-proxy_dc_creator;" + x(6),
      "d" + x(2) + "-proxy_dc_creator;" + x(6),
      "c" + x(1) + "-proxy_dc_publisher;" + x(7),
      "d" + x(2) + "-proxy_dc_publisher;" + x(7),
      "c" + x(1) + "-proxy_dc_contributor;" + x(8),
      "d" + x(2) + "-proxy_dc_contributor;" + x(8),
      "c" + x(1) + "-proxy_dc_type;" + x(9),
      "d" + x(2) + "-proxy_dc_type;" + x(9),
      "c" + x(1) + "-proxy_dc_identifier;" + x(10),
      "d" + x(2) + "-proxy_dc_identifier;" + x(10),
      "c" + x(1) + "-proxy_dc_language;" + x(11),
      "d" + x(2) + "-proxy_dc_language;" + x(11),
      "c" + x(1) + "-proxy_dc_coverage;" + x(12),
      "d" + x(2) + "-proxy_dc_coverage;" + x(12),
      "c" + x(1) + "-proxy_dcterms_temporal;" + x(13),
      "d" + x(2) + "-proxy_dcterms_temporal;" + x(13),
      "c" + x(1) + "-proxy_dcterms_spatial;" + x(14),
      "d" + x(2) + "-proxy_dcterms_spatial;" + x(14),
      "c" + x(1) + "-proxy_dc_subject;" + x(15),
      "d" + x(2) + "-proxy_dc_subject;" + x(15),
      "c" + x(1) + "-proxy_dc_date;" + x(16),
      "d" + x(2) + "-proxy_dc_date;" + x(16),
      "c" + x(1) + "-proxy_dcterms_created;" + x(17),
      "d" + x(2) + "-proxy_dcterms_created;" + x(17),
      "c" + x(1) + "-proxy_dcterms_issued;" + x(18),
      "d" + x(2) + "-proxy_dcterms_issued;" + x(18),
      "c" + x(1) + "-proxy_dcterms_extent;" + x(19),
      "d" + x(2) + "-proxy_dcterms_extent;" + x(19),
      "c" + x(1) + "-proxy_dcterms_medium;" + x(20),
      "d" + x(2) + "-proxy_dcterms_medium;" + x(20),
      "c" + x(1) + "-proxy_dcterms_provenance;" + x(21),
      "d" + x(2) + "-proxy_dcterms_provenance;" + x(21),
      "c" + x(1) + "-proxy_dcterms_hasPart;" + x(22),
      "d" + x(2) + "-proxy_dcterms_hasPart;" + x(22),
      "c" + x(1) + "-proxy_dcterms_isPartOf;" + x(23),
      "d" + x(2) + "-proxy_dcterms_isPartOf;" + x(23),
      "c" + x(1) + "-proxy_dc_format;" + x(24),
      "d" + x(2) + "-proxy_dc_format;" + x(24),
      "c" + x(1) + "-proxy_dc_source;" + x(25),
      "d" + x(2) + "-proxy_dc_source;" + x(25),
      "c" + x(1) + "-proxy_dc_rights;" + x(26),
      "d" + x(2) + "-proxy_dc_rights;" + x(26),
      "c" + x(1) + "-proxy_dc_relation;" + x(27),
      "d" + x(2) + "-proxy_dc_relation;" + x(27),
      "c" + x(1) + "-proxy_edm_europeanaProxy;" + x(28),
      "d" + x(2) + "-proxy_edm_europeanaProxy;" + x(28),
      "c" + x(1) + "-proxy_edm_year;" + x(29),
      "d" + x(2) + "-proxy_edm_year;" + x(29),
      "c" + x(1) + "-proxy_edm_userTag;" + x(30),
      "d" + x(2) + "-proxy_edm_userTag;" + x(30),
      "c" + x(1) + "-proxy_ore_ProxyIn;" + x(31),
      "d" + x(2) + "-proxy_ore_ProxyIn;" + x(31),
      "c" + x(1) + "-proxy_ore_ProxyFor;" + x(32),
      "d" + x(2) + "-proxy_ore_ProxyFor;" + x(32),
      "c" + x(1) + "-proxy_dc_conformsTo;" + x(33),
      "d" + x(2) + "-proxy_dc_conformsTo;" + x(33),
      "c" + x(1) + "-proxy_dcterms_hasFormat;" + x(34),
      "d" + x(2) + "-proxy_dcterms_hasFormat;" + x(34),
      "c" + x(1) + "-proxy_dcterms_hasVersion;" + x(35),
      "d" + x(2) + "-proxy_dcterms_hasVersion;" + x(35),
      "c" + x(1) + "-proxy_dcterms_isFormatOf;" + x(36),
      "d" + x(2) + "-proxy_dcterms_isFormatOf;" + x(36),
      "c" + x(1) + "-proxy_dcterms_isReferencedBy;" + x(37),
      "d" + x(2) + "-proxy_dcterms_isReferencedBy;" + x(37),
      "c" + x(1) + "-proxy_dcterms_isReplacedBy;" + x(38),
      "d" + x(2) + "-proxy_dcterms_isReplacedBy;" + x(38),
      "c" + x(1) + "-proxy_dcterms_isRequiredBy;" + x(39),
      "d" + x(2) + "-proxy_dcterms_isRequiredBy;" + x(39),
      "c" + x(1) + "-proxy_dcterms_isVersionOf;" + x(40),
      "d" + x(2) + "-proxy_dcterms_isVersionOf;" + x(40),
      "c" + x(1) + "-proxy_dcterms_references;" + x(41),
      "d" + x(2) + "-proxy_dcterms_references;" + x(41),
      "c" + x(1) + "-proxy_dcterms_replaces;" + x(42),
      "d" + x(2) + "-proxy_dcterms_replaces;" + x(42),
      "c" + x(1) + "-proxy_dcterms_requires;" + x(43),
      "d" + x(2) + "-proxy_dcterms_requires;" + x(43),
      "c" + x(1) + "-proxy_dcterms_tableOfContents;" + x(44),
      "d" + x(2) + "-proxy_dcterms_tableOfContents;" + x(44),
      "c" + x(1) + "-proxy_edm_currentLocation;" + x(45),
      "d" + x(2) + "-proxy_edm_currentLocation;" + x(45),
      "c" + x(1) + "-proxy_edm_hasMet;" + x(46),
      "d" + x(2) + "-proxy_edm_hasMet;" + x(46),
      "c" + x(1) + "-proxy_edm_hasType;" + x(47),
      "d" + x(2) + "-proxy_edm_hasType;" + x(47),
      "c" + x(1) + "-proxy_edm_incorporates;" + x(48),
      "d" + x(2) + "-proxy_edm_incorporates;" + x(48),
      "c" + x(1) + "-proxy_edm_isDerivativeOf;" + x(49),
      "d" + x(2) + "-proxy_edm_isDerivativeOf;" + x(49),
      "c" + x(1) + "-proxy_edm_isRelatedTo;" + x(50),
      "d" + x(2) + "-proxy_edm_isRelatedTo;" + x(50),
      "c" + x(1) + "-proxy_edm_isRepresentationOf;" + x(51),
      "d" + x(2) + "-proxy_edm_isRepresentationOf;" + x(51),
      "c" + x(1) + "-proxy_edm_isSimilarTo;" + x(52),
      "d" + x(2) + "-proxy_edm_isSimilarTo;" + x(52),
      "c" + x(1) + "-proxy_edm_isSuccessorOf;" + x(53),
      "d" + x(2) + "-proxy_edm_isSuccessorOf;" + x(53),
      "c" + x(1) + "-proxy_edm_realizes;" + x(54),
      "d" + x(2) + "-proxy_edm_realizes;" + x(54),
      "c" + x(1) + "-proxy_edm_wasPresentAt;" + x(55),
      "d" + x(2) + "-proxy_edm_wasPresentAt;" + x(55),
      "c" + x(1) + "-aggregation_edm_rights;" + x(56),
      "d" + x(2) + "-aggregation_edm_rights;" + x(56),
      "c" + x(1) + "-aggregation_edm_provider;" + x(57),
      "d" + x(2) + "-aggregation_edm_provider;" + x(57),
      "c" + x(1) + "-aggregation_edm_dataProvider;" + x(58),
      "d" + x(2) + "-aggregation_edm_dataProvider;" + x(58),
      "c" + x(1) + "-aggregation_dc_rights;" + x(59),
      "d" + x(2) + "-aggregation_dc_rights;" + x(59),
      "c" + x(1) + "-aggregation_edm_ugc;" + x(60),
      "d" + x(2) + "-aggregation_edm_ugc;" + x(60),
      "c" + x(1) + "-aggregation_edm_aggregatedCHO;" + x(61),
      "d" + x(2) + "-aggregation_edm_aggregatedCHO;" + x(61),
      "c" + x(1) + "-aggregation_edm_intermediateProvider;" + x(62),
      "d" + x(2) + "-aggregation_edm_intermediateProvider;" + x(62),
      "c" + x(1) + "-place_dcterms_isPartOf;" + x(63),
      "d" + x(2) + "-place_dcterms_isPartOf;" + x(63),
      "c" + x(1) + "-place_dcterms_hasPart;" + x(64),
      "d" + x(2) + "-place_dcterms_hasPart;" + x(64),
      "c" + x(1) + "-place_skos_prefLabel;" + x(65),
      "d" + x(2) + "-place_skos_prefLabel;" + x(65),
      "c" + x(1) + "-place_skos_altLabel;" + x(66),
      "d" + x(2) + "-place_skos_altLabel;" + x(66),
      "c" + x(1) + "-place_skos_note;" + x(67),
      "d" + x(2) + "-place_skos_note;" + x(67),
      "c" + x(1) + "-agent_edm_begin;" + x(68),
      "d" + x(2) + "-agent_edm_begin;" + x(68),
      "c" + x(1) + "-agent_edm_end;" + x(69),
      "d" + x(2) + "-agent_edm_end;" + x(69),
      "c" + x(1) + "-agent_edm_hasMet;" + x(70),
      "d" + x(2) + "-agent_edm_hasMet;" + x(70),
      "c" + x(1) + "-agent_edm_isRelatedTo;" + x(71),
      "d" + x(2) + "-agent_edm_isRelatedTo;" + x(71),
      "c" + x(1) + "-agent_owl_sameAs;" + x(72),
      "d" + x(2) + "-agent_owl_sameAs;" + x(72),
      "c" + x(1) + "-agent_foaf_name;" + x(73),
      "d" + x(2) + "-agent_foaf_name;" + x(73),
      "c" + x(1) + "-agent_dc_date;" + x(74),
      "d" + x(2) + "-agent_dc_date;" + x(74),
      "c" + x(1) + "-agent_dc_identifier;" + x(75),
      "d" + x(2) + "-agent_dc_identifier;" + x(75),
      "c" + x(1) + "-agent_rdaGr2_dateOfBirth;" + x(76),
      "d" + x(2) + "-agent_rdaGr2_dateOfBirth;" + x(76),
      "c" + x(1) + "-agent_rdaGr2_placeOfBirth;" + x(77),
      "d" + x(2) + "-agent_rdaGr2_placeOfBirth;" + x(77),
      "c" + x(1) + "-agent_rdaGr2_dateOfDeath;" + x(78),
      "d" + x(2) + "-agent_rdaGr2_dateOfDeath;" + x(78),
      "c" + x(1) + "-agent_rdaGr2_placeOfDeath;" + x(79),
      "d" + x(2) + "-agent_rdaGr2_placeOfDeath;" + x(79),
      "c" + x(1) + "-agent_rdaGr2_dateOfEstablishment;" + x(80),
      "d" + x(2) + "-agent_rdaGr2_dateOfEstablishment;" + x(80),
      "c" + x(1) + "-agent_rdaGr2_dateOfTermination;" + x(81),
      "d" + x(2) + "-agent_rdaGr2_dateOfTermination;" + x(81),
      "c" + x(1) + "-agent_rdaGr2_gender;" + x(82),
      "d" + x(2) + "-agent_rdaGr2_gender;" + x(82),
      "c" + x(1) + "-agent_rdaGr2_professionOrOccupation;" + x(83),
      "d" + x(2) + "-agent_rdaGr2_professionOrOccupation;" + x(83),
      "c" + x(1) + "-agent_rdaGr2_biographicalInformation;" + x(84),
      "d" + x(2) + "-agent_rdaGr2_biographicalInformation;" + x(84),
      "c" + x(1) + "-agent_skos_prefLabel;" + x(85),
      "d" + x(2) + "-agent_skos_prefLabel;" + x(85),
      "c" + x(1) + "-agent_skos_altLabel;" + x(86),
      "d" + x(2) + "-agent_skos_altLabel;" + x(86),
      "c" + x(1) + "-agent_skos_note;" + x(87),
      "d" + x(2) + "-agent_skos_note;" + x(87),
      "c" + x(1) + "-timespan_edm_begin;" + x(88),
      "d" + x(2) + "-timespan_edm_begin;" + x(88),
      "c" + x(1) + "-timespan_edm_end;" + x(89),
      "d" + x(2) + "-timespan_edm_end;" + x(89),
      "c" + x(1) + "-timespan_dcterms_isPartOf;" + x(90),
      "d" + x(2) + "-timespan_dcterms_isPartOf;" + x(90),
      "c" + x(1) + "-timespan_dcterms_hasPart;" + x(91),
      "d" + x(2) + "-timespan_dcterms_hasPart;" + x(91),
      "c" + x(1) + "-timespan_edm_isNextInSequence;" + x(92),
      "d" + x(2) + "-timespan_edm_isNextInSequence;" + x(92),
      "c" + x(1) + "-timespan_owl_sameAs;" + x(93),
      "d" + x(2) + "-timespan_owl_sameAs;" + x(93),
      "c" + x(1) + "-timespan_skos_prefLabel;" + x(94),
      "d" + x(2) + "-timespan_skos_prefLabel;" + x(94),
      "c" + x(1) + "-timespan_skos_altLabel;" + x(95),
      "d" + x(2) + "-timespan_skos_altLabel;" + x(95),
      "c" + x(1) + "-timespan_skos_note;" + x(96),
      "d" + x(2) + "-timespan_skos_note;" + x(96),
      "c" + x(1) + "-concept_skos_broader;" + x(97),
      "d" + x(2) + "-concept_skos_broader;" + x(97),
      "c" + x(1) + "-concept_skos_narrower;" + x(98),
      "d" + x(2) + "-concept_skos_narrower;" + x(98),
      "c" + x(1) + "-concept_skos_related;" + x(99),
      "d" + x(2) + "-concept_skos_related;" + x(99),
      "c" + x(1) + "-concept_skos_broadMatch;" + x(100),
      "d" + x(2) + "-concept_skos_broadMatch;" + x(100),
      "c" + x(1) + "-concept_skos_narrowMatch;" + x(101),
      "d" + x(2) + "-concept_skos_narrowMatch;" + x(101),
      "c" + x(1) + "-concept_skos_relatedMatch;" + x(102),
      "d" + x(2) + "-concept_skos_relatedMatch;" + x(102),
      "c" + x(1) + "-concept_skos_exactMatch;" + x(103),
      "d" + x(2) + "-concept_skos_exactMatch;" + x(103),
      "c" + x(1) + "-concept_skos_closeMatch;" + x(104),
      "d" + x(2) + "-concept_skos_closeMatch;" + x(104),
      "c" + x(1) + "-concept_skos_notation;" + x(105),
      "d" + x(2) + "-concept_skos_notation;" + x(105),
      "c" + x(1) + "-concept_skos_inScheme;" + x(106),
      "d" + x(2) + "-concept_skos_inScheme;" + x(106),
      "c" + x(1) + "-concept_skos_prefLabel;" + x(107),
      "d" + x(2) + "-concept_skos_prefLabel;" + x(107),
      "c" + x(1) + "-concept_skos_altLabel;" + x(108),
      "d" + x(2) + "-concept_skos_altLabel;" + x(108),
      "c" + x(1) + "-concept_skos_note;" + x(109),
      "d" + x(2) + "-concept_skos_note;" + x(109)
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

    /*
    language5.
      map(x => x._1.replace(".", ",") + "," + x._2). // -> "title,en,8"
      saveAsTextFile(dir + "/languages-per-collections-e.csv")
    */

    val language6 = language5.
      map(x => (x._1.split("\\."), x._2)).      // -> ((title, en), 8)
      map(x => (x._1.head, (x._1.last, x._2))). // -> (title, (en, 8))
      groupByKey()                             // -> (title, ((en, 8), (de, 5), ...))

    val language7 = language6.
      map(x => (x._1.split("-"), x._2)).      // -> ((title, en), 8)
      map(x => (x._1.head, (x._1.last, x._2))). // -> (title, (en, 8))
      groupByKey()                             // -> (title, ((en, 8), (de, 5), ...))

    language7.saveAsTextFile(dir + "/languages-per-collections-groupped.txt")
  }
}
