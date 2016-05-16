import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Languages {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Languages")
    val sc = new SparkContext(conf)

    val dir = args(0);
    val sourceFile = dir + "/" + args(1);

    // val sourceFile = "hdfs://localhost:54310/join/result11-languages.csv"
    val language = sc.textFile(sourceFile).filter(_.nonEmpty)

    val language2 = language
      .map(line => line
                     .replace(",", "','")
                     .concat("'")
                     .replace("''", "_00_")
                     .split(",")
                     .map(elem => elem.replaceAll("'", "")))

    val language3 = language2.flatMap(x => List(
      "identifier;" + x(3),
      "proxy_dc_title;" + x(4),
      "proxy_dcterms_alternative;" + x(5),
      "proxy_dc_description;" + x(6),
      "proxy_dc_creator;" + x(7),
      "proxy_dc_publisher;" + x(8),
      "proxy_dc_contributor;" + x(9),
      "proxy_dc_type;" + x(10),
      "proxy_dc_identifier;" + x(11),
      "proxy_dc_language;" + x(12),
      "proxy_dc_coverage;" + x(13),
      "proxy_dcterms_temporal;" + x(14),
      "proxy_dcterms_spatial;" + x(15),
      "proxy_dc_subject;" + x(16),
      "proxy_dc_date;" + x(17),
      "proxy_dcterms_created;" + x(18),
      "proxy_dcterms_issued;" + x(19),
      "proxy_dcterms_extent;" + x(20),
      "proxy_dcterms_medium;" + x(21),
      "proxy_dcterms_provenance;" + x(22),
      "proxy_dcterms_hasPart;" + x(23),
      "proxy_dcterms_isPartOf;" + x(24),
      "proxy_dc_format;" + x(25),
      "proxy_dc_source;" + x(26),
      "proxy_dc_rights;" + x(27),
      "proxy_dc_relation;" + x(28),
      "proxy_edm_isNextInSequence;" + x(29),
      "proxy_edm_rights;" + x(30),
      "aggregation_edm_rights;" + x(31),
      "aggregation_edm_provider;" + x(32),
      "aggregation_edm_dataProvider;" + x(33),
      "aggregation_edm_isShownAt;" + x(34),
      "aggregation_edm_isShownBy;" + x(35),
      "aggregation_edm_object;" + x(36),
      "aggregation_edm_hasView;" + x(37)
    ))                             // -> title;en;de

    val language4 = language3
      .map(x => (x.split(";")))    // -> (title, en, de)
      .map(x => (x.head, x.tail))  // -> (title, (en, de))
      .flatMap(x => x._2
        .map(y => x._1 + "." + y)) // -> (title.en, title.de)
      .map(x => (x, 1))            // -> title.en 1, 1, 1, ....
      .reduceByKey(_ + _)          // -> title.en 8

    language4
      .map(x => x._1.replace(".", ",") + "," + x._2) // -> "title,en,8"
      .saveAsTextFile(dir + "/languages.csv")

    language4
      .map(x => (x._1.split("\\."), x._2))      // -> ((title, en), 8)
      .map(x => (x._1.head, (x._1.last, x._2))) // -> (title, (en, 8))
      .groupByKey()                             // -> (title, ((en, 8), (de, 5), ...))
      .saveAsTextFile(dir + "/languages-groupped.txt")
  }
}
