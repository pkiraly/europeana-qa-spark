import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SaturationStat {
  def main(args: Array[String]): Unit = {

    // val conf = new SparkConf().setAppName("SaturationStat")
    // val sc = new SparkContext(conf)

    import spark.implicits._

    val log = org.apache.log4j.LogManager.getLogger("SaturationStat")
    val spark = SparkSession.builder.appName("SaturationStat").getOrCreate()

    var inputFile = "hdfs://localhost:54310/join/result29-multilingual-saturation-light.csv";

    val dataWithoutHeader = spark.read
      .option("header", "false")
      .option("inferSchema", "true")
      .format("csv")
      .load(inputFile)

    val id = Seq("id")
    val fields = Seq(
      "NumberOfLanguagesPerPropertyInProviderProxy", "NumberOfLanguagesPerPropertyInEuropeanaProxy",
        "NumberOfLanguagesPerPropertyInObject",
      "TaggedLiteralsInProviderProxy", "TaggedLiteralsInEuropeanaProxy",
        "DistinctLanguageCountInProviderProxy",
      "DistinctLanguageCountInEuropeanaProxy", "TaggedLiteralsInObject", "DistinctLanguagesInObject",
      "TaggedLiteralsPerLanguageInProviderProxy", "TaggedLiteralsPerLanguageInEuropeanaProxy",
        "TaggedLiteralsPerLanguageInObject"
    )
    val names = id ++ fields
    val data = dataWithoutHeader.toDF(names: _*).select(
      "NumberOfLanguagesPerPropertyInProviderProxy", "NumberOfLanguagesPerPropertyInEuropeanaProxy"
    )
    data.cache()

    var stat = data.select("NumberOfLanguagesPerPropertyInProviderProxy").describe()
    stat.write
      .option("header", "true")
      .csv("hdfs://localhost:54310/join/result29-multilingual-saturation-light-statistics")

  }
}

