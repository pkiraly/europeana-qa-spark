package de.gwdg.europeanaqa.spark.completeness

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.log4j._
import org.apache.spark.sql.functions.{udf, first, col}

// import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object Subdimensions {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val log = org.apache.log4j.LogManager.getLogger("Subdimensions")

    val spark = SparkSession.builder.
      appName("Subdimensions").
      getOrCreate()
    import spark.implicits._

    val parquetFile = args(0)
    log.info(s"reading parquetFile: $parquetFile")

    val df = spark.read.load(parquetFile)

    var filtered = df.select(
      "total", "mandatory", "descriptiveness", "searchability",
      "contextualization", "identification", "browsing",
      "viewing", "reusability", "multilinguality"
    )

    var described = filtered.describe()

    val cols = filtered.columns
    val fieldMap = cols.zipWithIndex.toMap
    val getFieldIndex = udf((field:String) => fieldMap(field))

    var transposed = described.
      flatMap(row => {
        row.getValuesMap(cols.filterNot(x => x == "summary")).
          keys.
          map(field =>
            (row.getString(0), field, row.getAs[String](field))
          )
      }).
      toDF("statistic", "field", "value").
      groupBy("field").
      pivot("statistic", Seq("count", "mean", "stddev", "min", "max")).
      agg(first("value"))

    var ordered = transposed.
      withColumn("index", getFieldIndex(col("field"))).
      sort("index").
      drop("index")

    ordered.
      repartition(1).
      write.
      option("header", "true").
      mode(SaveMode.Overwrite).
      csv("subdimensions-all-transformed")
  }
}
