import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object ExampleApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ExampleApp")
    val sc = new SparkContext(conf)

    val readmeFile = sc.textFile(args(0))
    val sparkMentions = readmeFile.filter(line => line.contains("Spark"))
    scala.tools.nsc.io.File(args(1)).writeAll(sparkMentions.count().toString)
  }
}

