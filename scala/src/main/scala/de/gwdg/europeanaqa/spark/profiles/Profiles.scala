package de.gwdg.europeanaqa.spark.profiles

import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets
import scala.collection.mutable.ListBuffer
import org.apache.log4j._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
// import org.apache.spark.ml.regression.LinearRegression
// import spark.implicits._

object Profiles {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Profiles")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder.getOrCreate()
    import spark.implicits._

    Logger.getLogger("org").setLevel(Level.ERROR)

    // var parquetFile = "hdfs://localhost:54310/join/completeness2.parquet"
    var parquetFile = args(0)
    val df = spark.read.load(parquetFile)
    val nrOfRecords = df.count()
    var simplenames = df.columns

    var len = simplenames.length
    var range = Range(1, (len-1))

    var providers = df.select("provider").distinct().orderBy("provider").collect()

    for (providerId <- providers) {
      var pid = providerId(0)
      var pairs = df.filter(s"provider == $pid").map{ r =>
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
      var participatingFieldsArray = counts.map{ case(pair,count) =>
        var parts = pair.split(",");
        var fields = new ListBuffer[Int]();
        for (i <- parts) {
          fields += i.toInt
        }
        fields.toList}.flatMap(identity)
      var participatingFields = sc.parallelize(participatingFieldsArray).toDF()
      var f = (participatingFields
        .groupBy("value")
        .count()
        .orderBy("value")
        .map(row => simplenames(row.getInt(0)))
        .collect()
        .mkString(";")
      )
      Files.write(Paths.get(s"profiles/d$pid-fields.csv"), f.getBytes(StandardCharsets.UTF_8))

      var edges = counts.map{ case(pair,count) =>
        var parts = pair.split(",");
        var fields = new ListBuffer[String]();
        for (i <- parts) {
          fields += simplenames(i.toInt)
        }
        (fields.toList.mkString(";"), fields.toList.length, count, (count.toFloat / nrOfRecords))
      }

      val names = Seq("profile", "nr-of-fields", "occurence", "percent")
      val df2 = sc.parallelize(edges).toDF(names: _*)
      var profilesFile = s"hdfs://localhost:54310/join/profiles/d${pid}-profiles.csv"
      df2.orderBy($"occurence".desc).write.csv(profilesFile)
    }
  }
}

