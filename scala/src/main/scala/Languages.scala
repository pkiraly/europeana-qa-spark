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
      "aggregation_edm_rights;" + x(28),
      "aggregation_edm_provider;" + x(29),
      "aggregation_edm_dataProvider;" + x(30)
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
      map(x => (x._1.head, (Integer.parseInt(x._1.last) * x._2)))

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
