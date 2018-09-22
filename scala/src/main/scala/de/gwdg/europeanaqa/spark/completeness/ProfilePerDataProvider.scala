package de.gwdg.europeanaqa.spark.completeness

import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.functions.desc

object ProfilePerDataProvider {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder.
      appName("ProfilePerDataProvider").
      getOrCreate()
    import spark.implicits._

    val parquetFile = args(0)
    log.info(s"ProfilePerDataProvider $parquetFile")

    val df = spark.read.load(parquetFile)
    val nrOfRecords = df.count()
    var simplenames = df.columns

    var len = simplenames.length
    var range = Range(1, (len - 1))

    var providers = df.select("provider").distinct().orderBy("provider").collect()

    for (providerId <- providers) {
      var pid = providerId(0)

      var pairs = df.filter(s"provider == $pid").map { r =>
        var fields = new ListBuffer[Int]();
        for (i <- range) {
          var value = r.getInt(i)
          if (value == 1) {
            fields += i
          }
        }
        fields.toList.mkString(",")
      }

      var counts = pairs.groupByKey(identity).count().collect()
      var participatingFieldsArray = counts.map { case (pair, count) =>
        var parts = pair.split(",");
        var fields = new ListBuffer[Int]();
        for (i <- parts) {
          fields += i.toInt
        }
        fields.toList
      }.flatMap(identity)
      var participatingFields = spark.sparkContext.
        parallelize(participatingFieldsArray).
        toDF()
      var f = participatingFields.
        groupBy("value").
        count().
        orderBy("value").
        map(row => simplenames(row.getInt(0))).
        collect().
        mkString(";")

      Files.write(Paths.get(s"d$pid-fields.csv"), f.getBytes(StandardCharsets.UTF_8))

      var edges = counts.map { case (pair, count) =>
        var parts = pair.split(",");
        var fields = new ListBuffer[String]();
        for (i <- parts) {
          fields += simplenames(i.toInt)
        }
        (fields.toList.mkString(";"),
          fields.toList.length,
          count,
          (count.toFloat / nrOfRecords))
      }

      val names = Seq("profile", "nr-of-fields", "occurence", "percent")
      val df2 = spark.sparkContext.parallelize(edges).toDF(names: _*)
      var profilesFile = s"hdfs://localhost:54310/join/d${pid}-profiles.csv"
      df2.orderBy(desc("_3")).write.csv(profilesFile)
    }
  }
}
