import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.first
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode

object SaturationWithHistogramForLight {

  def main(args: Array[String]): Unit = {

    val log = org.apache.log4j.LogManager.getLogger("SaturationStat")
    val spark = SparkSession.builder.appName("SaturationStat").getOrCreate()
    import spark.implicits._

    val configMap : Map[String, String] = spark.conf.getAll
    for ((key, value) <- configMap) {
      log.info(s"key: $key, value: $value")
    }

    val inputFile = args(0)
    val outputFile = args(1)

    log.info("reading the data")
    val dataWithoutHeader = spark.read.
      option("header", "false").
      option("inferSchema", "true").
      format("csv").
      load(inputFile)
    log.info("setting names")

    val id = Seq("id")
    val fields = Seq(
      "NumberOfLanguagesPerPropertyInProviderProxy", "NumberOfLanguagesPerPropertyInEuropeanaProxy", "NumberOfLanguagesPerPropertyInObject",
      "TaggedLiteralsInProviderProxy", "TaggedLiteralsInEuropeanaProxy", "DistinctLanguageCountInProviderProxy",
      "DistinctLanguageCountInEuropeanaProxy", "TaggedLiteralsInObject", "DistinctLanguageCountInObject",
      "TaggedLiteralsPerLanguageInProviderProxy", "TaggedLiteralsPerLanguageInEuropeanaProxy", "TaggedLiteralsPerLanguageInObject"
    )

    val names = id ++ fields
    val data = dataWithoutHeader.toDF(names: _*).select(fields.map(col): _*)
    data.cache()

    log.info("reading the data: done")

    data.printSchema()

    def toLongForm(df: DataFrame): DataFrame = {
      val schema = df.schema
      df.flatMap(row => {
        val metric = row.getString(0)
        (1 until row.size).map(i => {
          (metric, schema.fieldNames(i), row.getString(i).toDouble)
        })
      }).toDF("metric", "field", "value")
    }

    def getDouble(first: Row): Double = {
      if (first.schema.fields(0).dataType.equals(DoubleType)) {
        first.getDouble(0)
      } else {
        first.getInt(0).toDouble
      }
    }

    def getMedianFromHistogram(histogram: DataFrame, l: Long): Double = {
      var first = histogram.filter($"start" <= l && $"end" >= l)
                           .select("label")
                           .first()
      getDouble(first)
    }

    var total = data.count()
    var isImpair = total / 2 == 1

    var stat2 = Seq(("fake", "fake", 0.0)).toDF("metric", "field", "value")

    for (i <- 0 to (data.schema.fieldNames.size - 1)) {
      var l : Long = -1
      var r : Long = -1
      var median : Double = -1.0
      var zerosPerc : Double = -1.0
      var fieldName = data.schema.fieldNames(i);
      var dataType = data.schema.fields(i).dataType;
      log.info(s"calculating the median for $fieldName ($dataType)")

      var existing = data.filter(col(fieldName) > -1).select(fieldName)
      stat2 = stat2.union(toLongForm(existing.describe()))

      var histogram = data.select(fieldName).
        groupBy(fieldName).
        count().
        toDF("label", "count").
        orderBy("label").
        withColumn("group", functions.lit(1)).
        withColumn("end", sum("count").
          over(Window.partitionBy("group").orderBy($"label"))).
        withColumn("start", (col("end") - col("count"))).
        select("label", "count", "start", "end")

      histogram.write.
        option("header", "true").
        mode(SaveMode.Overwrite).
        csv(outputFile + "-histogram-" + fieldName)

      var lowest = histogram.select("label").first();
      if (dataType.equals(DoubleType))
        log.info("lowest: " + lowest.getDouble(0))
      else
        log.info("lowest: " + lowest.getInt(0))

      var zeros = histogram.select("count").first().getLong(0)
      zerosPerc = zeros * 100.0 / total

      if (isImpair) {
        l = (total / 2)
        r = l
        median = getMedianFromHistogram(histogram, l)
      } else {
        l = (total / 2) - 1
        r = l + 1
        var lval = getMedianFromHistogram(histogram, l)
        var rval = getMedianFromHistogram(histogram, r)
        median = (lval + rval) / 2
      }

      log.info(s"$fieldName: $median (zeros: $zerosPerc%)")

      stat2 = stat2.union(Seq(
        ("median", fieldName, median),
        ("zerosPerc", fieldName, zerosPerc)
      ).toDF("metric", "field", "value"))
    }

    val wideDf = stat2.
      filter(col("field") =!= "fake").
      groupBy("field").
      pivot("metric", Seq("count", "median", "zerosPerc", "mean", "stddev", "min", "max")).
      agg(first("value")).
      orderBy("field")

    wideDf.write.
      option("header", "true").
      mode(SaveMode.Overwrite).
      csv(outputFile)
  }
}

