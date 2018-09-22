package de.gwdg.europeanaqa.spark.completeness

import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets
import scala.collection.mutable.ListBuffer
import org.apache.log4j._

import org.apache.spark.SparkContext._
import org.apache.spark.sql.{SparkSession, Row, SaveMode}
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.col

object ProfilePerDataProviderWithMapping {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val log = org.apache.log4j.LogManager.getLogger(this.getClass().getCanonicalName)

    val spark = SparkSession.builder.
      appName(this.getClass().getCanonicalName).
      getOrCreate()
    import spark.implicits._

    val parquetFile = args(0)
    log.info(s"reading parquetFile: $parquetFile")

    val df = spark.read.load(parquetFile)
    val nrOfRecords = df.count()
    var simplenames = df.columns

    var len = simplenames.length
    var range = Range(2, (len - 1))

    var datasets = df.select("collection").distinct().orderBy("collection").collect()
    var providers = df.select("provider").distinct().orderBy("provider").collect()
    var total = providers.length

    def getFieldsByRow(row: Row): String = {
      var fields = new ListBuffer[Int]()
      for (i <- range) {
        var value = row.getInt(i)
        if (value == 1) {
          fields += i
        }
      }
      fields.toList.mkString(",")
    }

    def resolveFieldAbbreviation(pattern: String): String = {
      var parts = pattern.split(",");
      var fields = new ListBuffer[String]();
      for (i <- parts) {
        fields += simplenames(i.toInt)
      }
      fields.toList.mkString(";")
    }

    var dfCount = df.flatMap{row =>
        var cid = row.getAs[Int]("collection")
        var did = row.getAs[Int]("provider")
        var fields = getFieldsByRow(row)
        Seq(
          (s"c$cid", fields),
          (s"d$did", fields),
          (s"cd-$cid-$did", fields)
        )
      }.toDF(Seq("id","pattern"): _*).
      groupBy("id", "pattern").
      count().
      orderBy("id", "count")

    var countById = dfCount.groupBy("id").agg(sum("count").as("total")).
      toDF("id2", "total")

    var dfCountWithTotal = dfCount.
      join(countById, dfCount.col("id") === countById.col("id2"), "inner").
      withColumn("percent", col("count")*100/col("total")).
      select("id", "pattern", "count", "total", "percent")

    var resolved = dfCountWithTotal.map{row =>
        var id = row.getAs[String]("id")
        var count = row.getAs[Long]("count")
        var total = row.getAs[Long]("total")
        var percent = row.getAs[Double]("percent")
        var pattern = resolveFieldAbbreviation(row.getAs[String]("pattern"))
        (id, pattern, pattern.split(";").length, count, total, percent)
      }.
      toDF(Seq("id", "pattern", "length", "count", "total", "percent"): _*).
      orderBy("id", "count")

    def saveFields(id: Any, rows: Iterable[org.apache.spark.sql.Row]): Unit = {
      var fieldList = rows.flatMap(r => r.getAs[String]("pattern").split(";")).groupBy(x => x).map(x => x._1).mkString(";")
      Files.write(Paths.get(s"profiles/$id-fields.csv"), fieldList.getBytes(StandardCharsets.UTF_8))
    }

    def saveProfiles(id: Any, rows: Iterable[org.apache.spark.sql.Row]): Unit = {
      var patterns = rows.map{row =>
        (
          row.getAs[String]("pattern"),
          row.getAs[Int]("length"),
          row.getAs[Long]("total"),
          row.getAs[Double]("percent")
        )
      }.toList
      var patternsDF = spark.sparkContext.parallelize(patterns).
        toDF(Seq("profile", "nr-of-fields", "occurence", "percent"): _*).
        orderBy(desc("occurence"))

      var profilesFile = s"profiles-raw/${id}-profiles-csv"
      patternsDF.
        write.
        mode(SaveMode.Overwrite).
        csv(profilesFile)
    }

    def saveResult(id: Any, rows: Iterable[org.apache.spark.sql.Row]): Unit = {
      println(s"Saving $id (${rows.size} patterns)")
      saveFields(id, rows)
      saveProfiles(id, rows)
    }

    resolved.rdd.
      groupBy(row => row(0)).
      foreach(r => saveResult(r._1, r._2))

    log.info("DONE")
  }
}
