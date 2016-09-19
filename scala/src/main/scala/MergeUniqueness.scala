import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object MergeUniqueness {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("MergeUniqueness")
    val sc = new SparkContext(conf)

    // "hdfs://localhost:54310/join/result11.csv"
    System.out.println(args(0))
    val csv = sc.textFile(args(0)).filter(_.nonEmpty)
    val data = csv
      .map(line => line
                     .split(",")
                     .map(elem => elem.trim))
      .map(x => (x(0), x.mkString(",")))
    data.cache()

    val tfidfFile = sc.textFile("hdfs://localhost:54310/join/tfidf.csv").filter(_.nonEmpty)
    val tfidf = tfidfFile
      .map(line => line
                     .split(",")
                     .map(elem => elem.trim))
      .map(x => (x(0), x(1) + "," + x(2) + "," + x(3) + "," + x(4) + "," + x(5) + "," + x(6)))
    tfidf.cache()

    selectAndSave(args(1), tfidf, data)
    /*
    selectAndSave("0", tfidf, data)
    selectAndSave("1", tfidf, data)
    selectAndSave("2020", tfidf, data)
    selectAndSave("2021", tfidf, data)
    selectAndSave("2022", tfidf, data)
    selectAndSave("2023", tfidf, data)
    selectAndSave("2024", tfidf, data)
    selectAndSave("2025", tfidf, data)
    selectAndSave("2026", tfidf, data)
    selectAndSave("203", tfidf, data)
    selectAndSave("204", tfidf, data)
    selectAndSave("205", tfidf, data)
    selectAndSave("90", tfidf, data)
    selectAndSave("91", tfidf, data)
    selectAndSave("92", tfidf, data)
    */
  }

  def selectAndSave(filt:String, tfidf:RDD[(String, String)], data:RDD[(String, String)]) = {
    val tfidfFiltered = tfidf.filter(x => x._1.startsWith(filt))
    val dataFiltered = data.filter(x => x._1.startsWith(filt))
    val united = dataFiltered.union(tfidfFiltered)
    val merged = united
      .groupByKey()
      .map(x => 
        if (x._2.toList.head != x._2.toList.last) {
          if (x._2.toList.head.length > x._2.toList.last.length) {
            x._2.toList.mkString(",")
          } else {
            x._2.toList.last + "," + x._2.toList.head
          }
        }
      )
    merged.saveAsTextFile("hdfs://localhost:54310/join/merged-" + filt + ".csv")
  }
}
