import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Frequency {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Frequency")
    val sc = new SparkContext(conf)

    // "hdfs://localhost:54310/join/result11.csv"
    val csv = sc.textFile(args(0)).filter(_.nonEmpty)
    val count = csv.count()
    val data = csv.map(line => line.split(",").map(elem => elem.trim)) //lines in rows

    val frequencies = data
      .flatMap(x => List(

        "total." + x(3),
        "mandatory" + x(4),
        "descriptiveness" + x(5),
        "searchability" + x(6),
        "contextualization" + x(7),
        "identification" + x(8),
        "browsing" + x(9),
        "viewing" + x(10),
        "reusability" + x(11),
        "multilinguality" + x(12),

        "identifier." + x(13),
        "proxy_dc_title." + x(14),
        "proxy_dcterms_alternative." + x(15),
        "proxy_dc_description." + x(16),
        "proxy_dc_creator." + x(17),
        "proxy_dc_publisher." + x(18),
        "proxy_dc_contributor." + x(19),
        "proxy_dc_type." + x(20),
        "proxy_dc_identifier." + x(21),
        "proxy_dc_language." + x(22),
        "proxy_dc_coverage." + x(23),
        "proxy_dcterms_temporal." + x(24),
        "proxy_dcterms_spatial." + x(25),
        "proxy_dc_subject." + x(26),
        "proxy_dc_date." + x(27),
        "proxy_dcterms_created." + x(28),
        "proxy_dcterms_issued." + x(29),
        "proxy_dcterms_extent." + x(30),
        "proxy_dcterms_medium." + x(31),
        "proxy_dcterms_provenance." + x(32),
        "proxy_dcterms_hasPart." + x(33),
        "proxy_dcterms_isPartOf." + x(34),
        "proxy_dc_format." + x(35),
        "proxy_dc_source." + x(36),
        "proxy_dc_rights." + x(37),
        "proxy_dc_relation." + x(38),
        "proxy_edm_isNextInSequence." + x(39),
        "proxy_edm_type." + x(40),
        "aggregation_edm_rights." + x(41),
        "aggregation_edm_provider." + x(42),
        "aggregation_edm_dataProvider." + x(43),
        "aggregation_edm_isShownAt." + x(44),
        "aggregation_edm_isShownBy." + x(45),
        "aggregation_edm_object." + x(46),
        "aggregation_edm_hasView." + x(47)
      ))
      .map(x => (x, 1))
      .reduceByKey(_ + _)

    val freqData = frequencies
      .map(x => (x._1.split("\\."), x._2))
      .map(x => (x._1.head, (x._1.last, x._2)))
      .groupByKey()
      .map(x => (x._1, x._2.toMap))
      .map{ case(field, map) => (field, map.getOrElse("0", 0), map.getOrElse("1", 0)) }
      .map(x => (x._1, x._3, (x._3.toFloat / (x._2 + x._3))))

    // "hdfs://localhost:54310/join/frequency.csv"
    freqData.map(x => x._1 + "," + x._2 + "," + x._3).saveAsTextFile(args(1))
  }
}

