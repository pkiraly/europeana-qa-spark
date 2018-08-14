import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Row

object SaturationWithHistogram {

  def main(args: Array[String]): Unit = {

    // val conf = new SparkConf().setAppName("SaturationStat")
    // val sc = new SparkContext(conf)

    val log = org.apache.log4j.LogManager.getLogger("SaturationStat")
    val spark = SparkSession.builder.appName("SaturationStat").getOrCreate()

    import spark.implicits._

    val inputFile = args(0)
    // val inputFile = "/scratch/pkiraly/multilinguality/result29-multilingual-saturation-light.csv.gz"
    val outputFile = args(1)
    // val outputFile = "/scratch/pkiraly/multilinguality/result29-multilingual-saturation-light-statistics"
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

    def getDouble(first: Row): Double = {
      if (first.schema.fields(0).dataType.equals(DoubleType)) {
        first.getDouble(0)
      } else {
        first.getInt(0).toDouble
      }
    }

    var count = data.count()
    var isImpair = count / 2 == 1
    var medianRow = Seq.empty[Any]
    medianRow = medianRow :+ "median"

    for (i <- 0 to fields.size) {
      var l : Long = -1
      var r : Long = -1
      var median : Double = -1.0
      var fieldName = data.schema.fieldNames(i);

      var histogram = data.select(fieldName)
        .groupBy(fieldName)
        .count()
        .toDF("label", "count")
        .orderBy("label")
        .withColumn("group", functions.lit(1))
        .withColumn("end", sum("count")
          .over(Window.partitionBy("group").orderBy($"label")))
        .withColumn("start", (col("end") - col("count")))

      if (isImpair) {
        l = (count / 2)
        r = l
        median = getDouble(histogram.filter($"start" <= l && $"end" >= l).select("label").first())
      } else {
        l = (count / 2) - 1
        r = l + 1
        var lval = getDouble(histogram.filter($"start" <= l && $"end" >= l).select("label").first())
        var rval = getDouble(histogram.filter($"start" <= r && $"end" >= r).select("label").first())
        median = (lval + rval) / 2
      }

      medianRow = medianRow :+ median
    }

    val labels = Seq("summary") ++ orderedFields
    var strmedian = medianRow.map(x => x.toString)
    log.info(labels.size())
    log.info(medianRow.size())
    log.info(strmedian.size())
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

