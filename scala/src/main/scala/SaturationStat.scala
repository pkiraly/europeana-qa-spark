import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SaturationStat {
  def main(args: Array[String]): Unit = {

    // val conf = new SparkConf().setAppName("SaturationStat")
    // val sc = new SparkContext(conf)


    val log = org.apache.log4j.LogManager.getLogger("SaturationStat")
    val spark = SparkSession.builder.appName("SaturationStat").getOrCreate()
    import spark.implicits._

    var inputFile = args(0)
    var outputFile = args(1)
    // var inputFile = "hdfs://localhost:54310/join/result29-multilingual-saturation-light.csv";

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
      "NumberOfLanguagesPerPropertyInProviderProxy",
      "NumberOfLanguagesPerPropertyInEuropeanaProxy",
      "NumberOfLanguagesPerPropertyInObject",
      "TaggedLiteralsInProviderProxy",
      "TaggedLiteralsInEuropeanaProxy",
      "TaggedLiteralsInObject",
      "DistinctLanguageCountInProviderProxy",
      "DistinctLanguageCountInEuropeanaProxy",
      "DistinctLanguagesInObject",
      "TaggedLiteralsPerLanguageInProviderProxy",
      "TaggedLiteralsPerLanguageInEuropeanaProxy",
      "TaggedLiteralsPerLanguageInObject"
    )
    data.cache()

    var stat = data
      .select(
        "NumberOfLanguagesPerPropertyInProviderProxy",
        "NumberOfLanguagesPerPropertyInEuropeanaProxy",
        "NumberOfLanguagesPerPropertyInObject",
        "TaggedLiteralsInProviderProxy",
        "TaggedLiteralsInEuropeanaProxy",
        "TaggedLiteralsInObject",
        "DistinctLanguageCountInProviderProxy",
        "DistinctLanguageCountInEuropeanaProxy",
        "DistinctLanguagesInObject",
        "TaggedLiteralsPerLanguageInProviderProxy",
        "TaggedLiteralsPerLanguageInEuropeanaProxy",
        "TaggedLiteralsPerLanguageInObject"
      )
      .describe()

    def medianD(inputList: List[Double]): Double = {
      val count = inputList.size
      if (count % 2 == 0) {
        val l = count / 2 - 1
        val r = l + 1
        (inputList(l) + inputList(r)).toDouble / 2
      } else
        inputList(count / 2).toDouble
    }

    def medianI(inputList: List[Int]): Double = {
      val count = inputList.size
      if (count % 2 == 0) {
        val l = count / 2 - 1
        val r = l + 1
        (inputList(l) + inputList(r)).toDouble / 2
      } else
        inputList(count / 2).toDouble
    }

    var orderedFields = Seq(
      "NumberOfLanguagesPerPropertyInProviderProxy",
      "NumberOfLanguagesPerPropertyInEuropeanaProxy",
      "NumberOfLanguagesPerPropertyInObject",
      "TaggedLiteralsInProviderProxy",
      "TaggedLiteralsInEuropeanaProxy",
      "TaggedLiteralsInObject",
      "DistinctLanguageCountInProviderProxy",
      "DistinctLanguageCountInEuropeanaProxy",
      "DistinctLanguagesInObject",
      "TaggedLiteralsPerLanguageInProviderProxy",
      "TaggedLiteralsPerLanguageInEuropeanaProxy",
      "TaggedLiteralsPerLanguageInObject"
    )

    var medianRow = Seq.empty[Any]
    medianRow = medianRow :+ "median"

    val total = data.count
    for (field <- orderedFields) {
      // println(field)
      var median = 0.0;
      var max = 0.0;
      var percent = 0.0
      if (field.startsWith("NumberOfLanguages") || field.startsWith("TaggedLiteralsPerLanguage")) {
        percent = (data.select(field).filter(x => x.getDouble(0) > 0).count() * 100 / total)
        if (percent > 50.0) {
          val sortedList = data.select(field).sort(field).collect().map(r => r.getDouble(0)).toList
          max = sortedList.last
          median = medianD(sortedList)
        }
      } else {
        percent = (data.select(field).filter(x => x.getInt(0) > 0).count() * 100.0 / total)
        if (percent > 50.0) {
          val sortedList = data.select(field).sort(field).collect().map(r => r.getInt(0)).toList
          max = sortedList.last
          median = medianI(sortedList)
        }
      }
      medianRow = medianRow :+ median
      // println(s"$field: $med (max: $max, $percent% above 0)")
    }

    val labels = Seq("summary") ++ orderedFields
    var strmedian = medianRow.map(x => x.toString)
    val medianDf = Seq(strmedian).map(
      x => (
        x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12)
      )
    ).toDF(labels: _*)
    stat = stat.union(medianDf)

    stat.write
      .option("header", "true")
      .csv(outputFile) // "hdfs://localhost:54310/join/result29-multilingual-saturation-light-statistics"

  }
}

