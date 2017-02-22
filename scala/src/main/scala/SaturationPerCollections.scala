import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SaturationPerCollections {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SaturationPerCollections")
    val sc = new SparkContext(conf)

    val dir = args(0);
    val sourceFile = dir + "/" + args(1);
    val outputFile = dir + "/" + args(2);

    // val sourceFile = "hdfs://localhost:54310/join/result12-language.csv"
    val language = sc.textFile(sourceFile).filter(_.nonEmpty)

    val count = language.count().toDouble
    val language1 = language
      .map(line => line.split(","))

    val language2 = language1
      .flatMap(line => List(
        ("c" + line(1) + ":proxy_dc_title", line(3).toDouble),
        ("d" + line(2) + ":proxy_dc_title", line(3).toDouble),
        ("c" + line(1) + ":proxy_dcterms_alternative", line(4).toDouble),
        ("d" + line(2) + ":proxy_dcterms_alternative", line(4).toDouble),
        ("c" + line(1) + ":proxy_dc_description", line(5).toDouble),
        ("d" + line(2) + ":proxy_dc_description", line(5).toDouble),
        ("c" + line(1) + ":proxy_dc_creator", line(6).toDouble),
        ("d" + line(2) + ":proxy_dc_creator", line(6).toDouble),
        ("c" + line(1) + ":proxy_dc_publisher", line(7).toDouble),
        ("d" + line(2) + ":proxy_dc_publisher", line(7).toDouble),
        ("c" + line(1) + ":proxy_dc_contributor", line(8).toDouble),
        ("d" + line(2) + ":proxy_dc_contributor", line(8).toDouble),
        ("c" + line(1) + ":proxy_dc_type", line(9).toDouble),
        ("d" + line(2) + ":proxy_dc_type", line(9).toDouble),
        ("c" + line(1) + ":proxy_dc_identifier", line(10).toDouble),
        ("d" + line(2) + ":proxy_dc_identifier", line(10).toDouble),
        ("c" + line(1) + ":proxy_dc_language", line(11).toDouble),
        ("d" + line(2) + ":proxy_dc_language", line(11).toDouble),
        ("c" + line(1) + ":proxy_dc_coverage", line(12).toDouble),
        ("d" + line(2) + ":proxy_dc_coverage", line(12).toDouble),
        ("c" + line(1) + ":proxy_dcterms_temporal", line(13).toDouble),
        ("d" + line(2) + ":proxy_dcterms_temporal", line(13).toDouble),
        ("c" + line(1) + ":proxy_dcterms_spatial", line(14).toDouble),
        ("d" + line(2) + ":proxy_dcterms_spatial", line(14).toDouble),
        ("c" + line(1) + ":proxy_dc_subject", line(15).toDouble),
        ("d" + line(2) + ":proxy_dc_subject", line(15).toDouble),
        ("c" + line(1) + ":proxy_dc_date", line(16).toDouble),
        ("d" + line(2) + ":proxy_dc_date", line(16).toDouble),
        ("c" + line(1) + ":proxy_dcterms_created", line(17).toDouble),
        ("d" + line(2) + ":proxy_dcterms_created", line(17).toDouble),
        ("c" + line(1) + ":proxy_dcterms_issued", line(18).toDouble),
        ("d" + line(2) + ":proxy_dcterms_issued", line(18).toDouble),
        ("c" + line(1) + ":proxy_dcterms_extent", line(19).toDouble),
        ("d" + line(2) + ":proxy_dcterms_extent", line(19).toDouble),
        ("c" + line(1) + ":proxy_dcterms_medium", line(20).toDouble),
        ("d" + line(2) + ":proxy_dcterms_medium", line(20).toDouble),
        ("c" + line(1) + ":proxy_dcterms_provenance", line(21).toDouble),
        ("d" + line(2) + ":proxy_dcterms_provenance", line(21).toDouble),
        ("c" + line(1) + ":proxy_dcterms_hasPart", line(22).toDouble),
        ("d" + line(2) + ":proxy_dcterms_hasPart", line(22).toDouble),
        ("c" + line(1) + ":proxy_dcterms_isPartOf", line(23).toDouble),
        ("d" + line(2) + ":proxy_dcterms_isPartOf", line(23).toDouble),
        ("c" + line(1) + ":proxy_dc_format", line(24).toDouble),
        ("d" + line(2) + ":proxy_dc_format", line(24).toDouble),
        ("c" + line(1) + ":proxy_dc_source", line(25).toDouble),
        ("d" + line(2) + ":proxy_dc_source", line(25).toDouble),
        ("c" + line(1) + ":proxy_dc_rights", line(26).toDouble),
        ("d" + line(2) + ":proxy_dc_rights", line(26).toDouble),
        ("c" + line(1) + ":proxy_dc_relation", line(27).toDouble),
        ("d" + line(2) + ":proxy_dc_relation", line(27).toDouble),
        ("c" + line(1) + ":proxy_edm_europeanaProxy", line(28).toDouble),
        ("d" + line(2) + ":proxy_edm_europeanaProxy", line(28).toDouble),
        ("c" + line(1) + ":proxy_edm_year", line(29).toDouble),
        ("d" + line(2) + ":proxy_edm_year", line(29).toDouble),
        ("c" + line(1) + ":proxy_edm_userTag", line(30).toDouble),
        ("d" + line(2) + ":proxy_edm_userTag", line(30).toDouble),
        ("c" + line(1) + ":proxy_ore_ProxyIn", line(31).toDouble),
        ("d" + line(2) + ":proxy_ore_ProxyIn", line(31).toDouble),
        ("c" + line(1) + ":proxy_ore_ProxyFor", line(32).toDouble),
        ("d" + line(2) + ":proxy_ore_ProxyFor", line(32).toDouble),
        ("c" + line(1) + ":proxy_dc_conformsTo", line(33).toDouble),
        ("d" + line(2) + ":proxy_dc_conformsTo", line(33).toDouble),
        ("c" + line(1) + ":proxy_dcterms_hasFormat", line(34).toDouble),
        ("d" + line(2) + ":proxy_dcterms_hasFormat", line(34).toDouble),
        ("c" + line(1) + ":proxy_dcterms_hasVersion", line(35).toDouble),
        ("d" + line(2) + ":proxy_dcterms_hasVersion", line(35).toDouble),
        ("c" + line(1) + ":proxy_dcterms_isFormatOf", line(36).toDouble),
        ("d" + line(2) + ":proxy_dcterms_isFormatOf", line(36).toDouble),
        ("c" + line(1) + ":proxy_dcterms_isReferencedBy", line(37).toDouble),
        ("d" + line(2) + ":proxy_dcterms_isReferencedBy", line(37).toDouble),
        ("c" + line(1) + ":proxy_dcterms_isReplacedBy", line(38).toDouble),
        ("d" + line(2) + ":proxy_dcterms_isReplacedBy", line(38).toDouble),
        ("c" + line(1) + ":proxy_dcterms_isRequiredBy", line(39).toDouble),
        ("d" + line(2) + ":proxy_dcterms_isRequiredBy", line(39).toDouble),
        ("c" + line(1) + ":proxy_dcterms_isVersionOf", line(40).toDouble),
        ("d" + line(2) + ":proxy_dcterms_isVersionOf", line(40).toDouble),
        ("c" + line(1) + ":proxy_dcterms_references", line(41).toDouble),
        ("d" + line(2) + ":proxy_dcterms_references", line(41).toDouble),
        ("c" + line(1) + ":proxy_dcterms_replaces", line(42).toDouble),
        ("d" + line(2) + ":proxy_dcterms_replaces", line(42).toDouble),
        ("c" + line(1) + ":proxy_dcterms_requires", line(43).toDouble),
        ("d" + line(2) + ":proxy_dcterms_requires", line(43).toDouble),
        ("c" + line(1) + ":proxy_dcterms_tableOfContents", line(44).toDouble),
        ("d" + line(2) + ":proxy_dcterms_tableOfContents", line(44).toDouble),
        ("c" + line(1) + ":proxy_edm_currentLocation", line(45).toDouble),
        ("d" + line(2) + ":proxy_edm_currentLocation", line(45).toDouble),
        ("c" + line(1) + ":proxy_edm_hasMet", line(46).toDouble),
        ("d" + line(2) + ":proxy_edm_hasMet", line(46).toDouble),
        ("c" + line(1) + ":proxy_edm_hasType", line(47).toDouble),
        ("d" + line(2) + ":proxy_edm_hasType", line(47).toDouble),
        ("c" + line(1) + ":proxy_edm_incorporates", line(48).toDouble),
        ("d" + line(2) + ":proxy_edm_incorporates", line(48).toDouble),
        ("c" + line(1) + ":proxy_edm_isDerivativeOf", line(49).toDouble),
        ("d" + line(2) + ":proxy_edm_isDerivativeOf", line(49).toDouble),
        ("c" + line(1) + ":proxy_edm_isRelatedTo", line(50).toDouble),
        ("d" + line(2) + ":proxy_edm_isRelatedTo", line(50).toDouble),
        ("c" + line(1) + ":proxy_edm_isRepresentationOf", line(51).toDouble),
        ("d" + line(2) + ":proxy_edm_isRepresentationOf", line(51).toDouble),
        ("c" + line(1) + ":proxy_edm_isSimilarTo", line(52).toDouble),
        ("d" + line(2) + ":proxy_edm_isSimilarTo", line(52).toDouble),
        ("c" + line(1) + ":proxy_edm_isSuccessorOf", line(53).toDouble),
        ("d" + line(2) + ":proxy_edm_isSuccessorOf", line(53).toDouble),
        ("c" + line(1) + ":proxy_edm_realizes", line(54).toDouble),
        ("d" + line(2) + ":proxy_edm_realizes", line(54).toDouble),
        ("c" + line(1) + ":proxy_edm_wasPresentAt", line(55).toDouble),
        ("d" + line(2) + ":proxy_edm_wasPresentAt", line(55).toDouble),
        ("c" + line(1) + ":aggregation_edm_rights", line(56).toDouble),
        ("d" + line(2) + ":aggregation_edm_rights", line(56).toDouble),
        ("c" + line(1) + ":aggregation_edm_provider", line(57).toDouble),
        ("d" + line(2) + ":aggregation_edm_provider", line(57).toDouble),
        ("c" + line(1) + ":aggregation_edm_dataProvider", line(58).toDouble),
        ("d" + line(2) + ":aggregation_edm_dataProvider", line(58).toDouble),
        ("c" + line(1) + ":aggregation_dc_rights", line(59).toDouble),
        ("d" + line(2) + ":aggregation_dc_rights", line(59).toDouble),
        ("c" + line(1) + ":aggregation_edm_ugc", line(60).toDouble),
        ("d" + line(2) + ":aggregation_edm_ugc", line(60).toDouble),
        ("c" + line(1) + ":aggregation_edm_aggregatedCHO", line(61).toDouble),
        ("d" + line(2) + ":aggregation_edm_aggregatedCHO", line(61).toDouble),
        ("c" + line(1) + ":aggregation_edm_intermediateProvider", line(62).toDouble),
        ("d" + line(2) + ":aggregation_edm_intermediateProvider", line(62).toDouble),
        ("c" + line(1) + ":place_dcterms_isPartOf", line(63).toDouble),
        ("d" + line(2) + ":place_dcterms_isPartOf", line(63).toDouble),
        ("c" + line(1) + ":place_dcterms_hasPart", line(64).toDouble),
        ("d" + line(2) + ":place_dcterms_hasPart", line(64).toDouble),
        ("c" + line(1) + ":place_skos_prefLabel", line(65).toDouble),
        ("d" + line(2) + ":place_skos_prefLabel", line(65).toDouble),
        ("c" + line(1) + ":place_skos_altLabel", line(66).toDouble),
        ("d" + line(2) + ":place_skos_altLabel", line(66).toDouble),
        ("c" + line(1) + ":place_skos_note", line(67).toDouble),
        ("d" + line(2) + ":place_skos_note", line(67).toDouble),
        ("c" + line(1) + ":agent_edm_begin", line(68).toDouble),
        ("d" + line(2) + ":agent_edm_begin", line(68).toDouble),
        ("c" + line(1) + ":agent_edm_end", line(69).toDouble),
        ("d" + line(2) + ":agent_edm_end", line(69).toDouble),
        ("c" + line(1) + ":agent_edm_hasMet", line(70).toDouble),
        ("d" + line(2) + ":agent_edm_hasMet", line(70).toDouble),
        ("c" + line(1) + ":agent_edm_isRelatedTo", line(71).toDouble),
        ("d" + line(2) + ":agent_edm_isRelatedTo", line(71).toDouble),
        ("c" + line(1) + ":agent_owl_sameAs", line(72).toDouble),
        ("d" + line(2) + ":agent_owl_sameAs", line(72).toDouble),
        ("c" + line(1) + ":agent_foaf_name", line(73).toDouble),
        ("d" + line(2) + ":agent_foaf_name", line(73).toDouble),
        ("c" + line(1) + ":agent_dc_date", line(74).toDouble),
        ("d" + line(2) + ":agent_dc_date", line(74).toDouble),
        ("c" + line(1) + ":agent_dc_identifier", line(75).toDouble),
        ("d" + line(2) + ":agent_dc_identifier", line(75).toDouble),
        ("c" + line(1) + ":agent_rdaGr2_dateOfBirth", line(76).toDouble),
        ("d" + line(2) + ":agent_rdaGr2_dateOfBirth", line(76).toDouble),
        ("c" + line(1) + ":agent_rdaGr2_placeOfBirth", line(77).toDouble),
        ("d" + line(2) + ":agent_rdaGr2_placeOfBirth", line(77).toDouble),
        ("c" + line(1) + ":agent_rdaGr2_dateOfDeath", line(78).toDouble),
        ("d" + line(2) + ":agent_rdaGr2_dateOfDeath", line(78).toDouble),
        ("c" + line(1) + ":agent_rdaGr2_placeOfDeath", line(79).toDouble),
        ("d" + line(2) + ":agent_rdaGr2_placeOfDeath", line(79).toDouble),
        ("c" + line(1) + ":agent_rdaGr2_dateOfEstablishment", line(80).toDouble),
        ("d" + line(2) + ":agent_rdaGr2_dateOfEstablishment", line(80).toDouble),
        ("c" + line(1) + ":agent_rdaGr2_dateOfTermination", line(81).toDouble),
        ("d" + line(2) + ":agent_rdaGr2_dateOfTermination", line(81).toDouble),
        ("c" + line(1) + ":agent_rdaGr2_gender", line(82).toDouble),
        ("d" + line(2) + ":agent_rdaGr2_gender", line(82).toDouble),
        ("c" + line(1) + ":agent_rdaGr2_professionOrOccupation", line(83).toDouble),
        ("d" + line(2) + ":agent_rdaGr2_professionOrOccupation", line(83).toDouble),
        ("c" + line(1) + ":agent_rdaGr2_biographicalInformation", line(84).toDouble),
        ("d" + line(2) + ":agent_rdaGr2_biographicalInformation", line(84).toDouble),
        ("c" + line(1) + ":agent_skos_prefLabel", line(85).toDouble),
        ("d" + line(2) + ":agent_skos_prefLabel", line(85).toDouble),
        ("c" + line(1) + ":agent_skos_altLabel", line(86).toDouble),
        ("d" + line(2) + ":agent_skos_altLabel", line(86).toDouble),
        ("c" + line(1) + ":agent_skos_note", line(87).toDouble),
        ("d" + line(2) + ":agent_skos_note", line(87).toDouble),
        ("c" + line(1) + ":timespan_edm_begin", line(88).toDouble),
        ("d" + line(2) + ":timespan_edm_begin", line(88).toDouble),
        ("c" + line(1) + ":timespan_edm_end", line(89).toDouble),
        ("d" + line(2) + ":timespan_edm_end", line(89).toDouble),
        ("c" + line(1) + ":timespan_dcterms_isPartOf", line(90).toDouble),
        ("d" + line(2) + ":timespan_dcterms_isPartOf", line(90).toDouble),
        ("c" + line(1) + ":timespan_dcterms_hasPart", line(91).toDouble),
        ("d" + line(2) + ":timespan_dcterms_hasPart", line(91).toDouble),
        ("c" + line(1) + ":timespan_edm_isNextInSequence", line(92).toDouble),
        ("d" + line(2) + ":timespan_edm_isNextInSequence", line(92).toDouble),
        ("c" + line(1) + ":timespan_owl_sameAs", line(93).toDouble),
        ("d" + line(2) + ":timespan_owl_sameAs", line(93).toDouble),
        ("c" + line(1) + ":timespan_skos_prefLabel", line(94).toDouble),
        ("d" + line(2) + ":timespan_skos_prefLabel", line(94).toDouble),
        ("c" + line(1) + ":timespan_skos_altLabel", line(95).toDouble),
        ("d" + line(2) + ":timespan_skos_altLabel", line(95).toDouble),
        ("c" + line(1) + ":timespan_skos_note", line(96).toDouble),
        ("d" + line(2) + ":timespan_skos_note", line(96).toDouble),
        ("c" + line(1) + ":concept_skos_broader", line(97).toDouble),
        ("d" + line(2) + ":concept_skos_broader", line(97).toDouble),
        ("c" + line(1) + ":concept_skos_narrower", line(98).toDouble),
        ("d" + line(2) + ":concept_skos_narrower", line(98).toDouble),
        ("c" + line(1) + ":concept_skos_related", line(99).toDouble),
        ("d" + line(2) + ":concept_skos_related", line(99).toDouble),
        ("c" + line(1) + ":concept_skos_broadMatch", line(100).toDouble),
        ("d" + line(2) + ":concept_skos_broadMatch", line(100).toDouble),
        ("c" + line(1) + ":concept_skos_narrowMatch", line(101).toDouble),
        ("d" + line(2) + ":concept_skos_narrowMatch", line(101).toDouble),
        ("c" + line(1) + ":concept_skos_relatedMatch", line(102).toDouble),
        ("d" + line(2) + ":concept_skos_relatedMatch", line(102).toDouble),
        ("c" + line(1) + ":concept_skos_exactMatch", line(103).toDouble),
        ("d" + line(2) + ":concept_skos_exactMatch", line(103).toDouble),
        ("c" + line(1) + ":concept_skos_closeMatch", line(104).toDouble),
        ("d" + line(2) + ":concept_skos_closeMatch", line(104).toDouble),
        ("c" + line(1) + ":concept_skos_notation", line(105).toDouble),
        ("d" + line(2) + ":concept_skos_notation", line(105).toDouble),
        ("c" + line(1) + ":concept_skos_inScheme", line(106).toDouble),
        ("d" + line(2) + ":concept_skos_inScheme", line(106).toDouble),
        ("c" + line(1) + ":concept_skos_prefLabel", line(107).toDouble),
        ("d" + line(2) + ":concept_skos_prefLabel", line(107).toDouble),
        ("c" + line(1) + ":concept_skos_altLabel", line(108).toDouble),
        ("d" + line(2) + ":concept_skos_altLabel", line(108).toDouble),
        ("c" + line(1) + ":concept_skos_note", line(109).toDouble),
        ("d" + line(2) + ":concept_skos_note", line(109).toDouble)
      ))
      // .groupByKey()
      // .
      .reduceByKey(_ + _)
      .map{case(field, sum) => (field.split(":"), sum/count)}
      .map{case(fields, sum) => (fields(0), fields(1), f"$sum%.6f")}

    language2.
      saveAsTextFile(outputFile)

/*
    val language3 = language2.flatMap(x => List(
      "proxy_dc_title;" + line(3),
      "proxy_dcterms_alternative;" + line(4),
      "proxy_dc_description;" + line(5),
      "proxy_dc_creator;" + line(6),
      "proxy_dc_publisher;" + line(7),
      "proxy_dc_contributor;" + line(8),
      "proxy_dc_type;" + line(9),
      "proxy_dc_identifier;" + line(10),
      "proxy_dc_language;" + line(11),
      "proxy_dc_coverage;" + line(12),
      "proxy_dcterms_temporal;" + line(13),
      "proxy_dcterms_spatial;" + line(14),
      "proxy_dc_subject;" + line(15),
      "proxy_dc_date;" + line(16),
      "proxy_dcterms_created;" + line(17),
      "proxy_dcterms_issued;" + line(18),
      "proxy_dcterms_extent;" + line(19),
      "proxy_dcterms_medium;" + line(20),
      "proxy_dcterms_provenance;" + line(21),
      "proxy_dcterms_hasPart;" + line(22),
      "proxy_dcterms_isPartOf;" + line(23),
      "proxy_dc_format;" + line(24),
      "proxy_dc_source;" + line(25),
      "proxy_dc_rights;" + line(26),
      "proxy_dc_relation;" + line(27),
      "proxy_edm_europeanaProxy;" + line(28),
      "proxy_edm_year;" + line(29),
      "proxy_edm_userTag;" + line(30),
      "proxy_ore_ProxyIn;" + line(31),
      "proxy_ore_ProxyFor;" + line(32),
      "proxy_dc_conformsTo;" + line(33),
      "proxy_dcterms_hasFormat;" + line(34),
      "proxy_dcterms_hasVersion;" + line(35),
      "proxy_dcterms_isFormatOf;" + line(36),
      "proxy_dcterms_isReferencedBy;" + line(37),
      "proxy_dcterms_isReplacedBy;" + line(38),
      "proxy_dcterms_isRequiredBy;" + line(39),
      "proxy_dcterms_isVersionOf;" + line(40),
      "proxy_dcterms_references;" + line(41),
      "proxy_dcterms_replaces;" + line(42),
      "proxy_dcterms_requires;" + line(43),
      "proxy_dcterms_tableOfContents;" + line(44),
      "proxy_edm_currentLocation;" + line(45),
      "proxy_edm_hasMet;" + line(46),
      "proxy_edm_hasType;" + line(47),
      "proxy_edm_incorporates;" + line(48),
      "proxy_edm_isDerivativeOf;" + line(49),
      "proxy_edm_isRelatedTo;" + line(50),
      "proxy_edm_isRepresentationOf;" + line(51),
      "proxy_edm_isSimilarTo;" + line(52),
      "proxy_edm_isSuccessorOf;" + line(53),
      "proxy_edm_realizes;" + line(54),
      "proxy_edm_wasPresentAt;" + line(55),
      "aggregation_edm_rights;" + line(56),
      "aggregation_edm_provider;" + line(57),
      "aggregation_edm_dataProvider;" + line(58),
      "aggregation_dc_rights;" + line(59),
      "aggregation_edm_ugc;" + line(60),
      "aggregation_edm_aggregatedCHO;" + line(61),
      "aggregation_edm_intermediateProvider;" + line(62),
      "place_dcterms_isPartOf;" + line(63),
      "place_dcterms_hasPart;" + line(64),
      "place_skos_prefLabel;" + line(65),
      "place_skos_altLabel;" + line(66),
      "place_skos_note;" + line(67),
      "agent_edm_begin;" + line(68),
      "agent_edm_end;" + line(69),
      "agent_edm_hasMet;" + line(70),
      "agent_edm_isRelatedTo;" + line(71),
      "agent_owl_sameAs;" + line(72),
      "agent_foaf_name;" + line(73),
      "agent_dc_date;" + line(74),
      "agent_dc_identifier;" + line(75),
      "agent_rdaGr2_dateOfBirth;" + line(76),
      "agent_rdaGr2_placeOfBirth;" + line(77),
      "agent_rdaGr2_dateOfDeath;" + line(78),
      "agent_rdaGr2_placeOfDeath;" + line(79),
      "agent_rdaGr2_dateOfEstablishment;" + line(80),
      "agent_rdaGr2_dateOfTermination;" + line(81),
      "agent_rdaGr2_gender;" + line(82),
      "agent_rdaGr2_professionOrOccupation;" + line(83),
      "agent_rdaGr2_biographicalInformation;" + line(84),
      "agent_skos_prefLabel;" + line(85),
      "agent_skos_altLabel;" + line(86),
      "agent_skos_note;" + line(87),
      "timespan_edm_begin;" + line(88),
      "timespan_edm_end;" + line(89),
      "timespan_dcterms_isPartOf;" + line(90),
      "timespan_dcterms_hasPart;" + line(91),
      "timespan_edm_isNextInSequence;" + line(92),
      "timespan_owl_sameAs;" + line(93),
      "timespan_skos_prefLabel;" + line(94),
      "timespan_skos_altLabel;" + line(95),
      "timespan_skos_note;" + line(96),
      "concept_skos_broader;" + line(97),
      "concept_skos_narrower;" + line(98),
      "concept_skos_related;" + line(99),
      "concept_skos_broadMatch;" + line(100),
      "concept_skos_narrowMatch;" + line(101),
      "concept_skos_relatedMatch;" + line(102),
      "concept_skos_exactMatch;" + line(103),
      "concept_skos_closeMatch;" + line(104),
      "concept_skos_notation;" + line(105),
      "concept_skos_inScheme;" + line(106),
      "concept_skos_prefLabel;" + line(107),
      "concept_skos_altLabel;" + line(108),
      "concept_skos_note;" + line(109)
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
      saveAsTextFile(dir + "/languages.csv")
*/
  }
}
