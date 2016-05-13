import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Cardinality {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Cardinality")
    val sc = new SparkContext(conf)

    // "hdfs://localhost:54310/join/result11.csv"
    val csv = sc.textFile(args(0)).filter(_.nonEmpty)
    val data = csv.map(line => line.split(",").map(elem => elem.trim)) //lines in rows

    val cardinality = data.flatMap(
      x => List(
        "identifier." + x(48),
        "proxy_dc_title." + x(49),
        "proxy_dcterms_alternative." + x(50),
        "proxy_dc_description." + x(51),
        "proxy_dc_creator." + x(52),
        "proxy_dc_publisher." + x(53),
        "proxy_dc_contributor." + x(54),
        "proxy_dc_type." + x(55),
        "proxy_dc_identifier." + x(56),
        "proxy_dc_language." + x(57),
        "proxy_dc_coverage." + x(58),
        "proxy_dcterms_temporal." + x(59),
        "proxy_dcterms_spatial." + x(60),
        "proxy_dc_subject." + x(61),
        "proxy_dc_date." + x(62),
        "proxy_dcterms_created." + x(63),
        "proxy_dcterms_issued." + x(64),
        "proxy_dcterms_extent." + x(65),
        "proxy_dcterms_medium." + x(66),
        "proxy_dcterms_provenance." + x(67),
        "proxy_dcterms_hasPart." + x(68),
        "proxy_dcterms_isPartOf." + x(69),
        "proxy_dc_format." + x(70),
        "proxy_dc_source." + x(71),
        "proxy_dc_rights." + x(72),
        "proxy_dc_relation." + x(73),
        "proxy_edm_isNextInSequence." + x(74),
        "proxy_edm_type." + x(75),
        "aggregation_edm_rights." + x(76),
        "aggregation_edm_provider." + x(77),
        "aggregation_edm_dataProvider." + x(78),
        "aggregation_edm_isShownAt." + x(79),
        "aggregation_edm_isShownBy." + x(80),
        "aggregation_edm_object." + x(81),
        "aggregation_edm_hasView." + x(82)
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
      .saveAsTextFile(args(1))

    cardinality
      .map(x => (x._1.head, (x._1.last, x._2)))
      .groupByKey()
      .mapValues(x => x.toMap)
      .map{ case(x, map) => (
      	x, 
      	map.getOrElse("0", 0), 
      	map.filter(x => x._1 != "0")
      	   .map(x => x._2)
      	   .reduce(_ + _)
      )}
      .map(x => (x._1, x._3, (x._3.toFloat / (x._2 + x._3))))
      .map(x => x._1 + "," + x._2 + "," + x._3)
      .saveAsTextFile(args(2)) // "hdfs://localhost:54310/join/frequency.csv"
  }
}

