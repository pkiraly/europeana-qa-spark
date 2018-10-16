package de.gwdg.europeanaqa.spark.common

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object HistogramHelper {

  case class Counter(value: Double, count: Double)
  case class HistogramUnit(from: Double, until: Double, count: Double)

  def listToHistogram(numbers: Seq[Double]): Seq[Counter] = {
    var values = numbers.zipWithIndex.filter(x => x._2 % 2 == 0).map(x => x._1)
    var counts = numbers.zipWithIndex.filter(x => x._2 % 2 == 1).map(x => x._1)
    values.zip(counts).map(x => Counter(x._1, x._2))
  }

  val histogramFields = StructType(List(
    StructField("id", StringType, nullable = false),
    StructField("field", IntegerType, nullable = false),
    StructField("histogram", StringType, nullable = false)
  ))

  def minifyHistogram(histogram: Seq[Counter]): Seq[HistogramUnit] = {
    var len = histogram.length
    var minified: Seq[HistogramUnit] = Seq()
    if (len <= 10) {
      for (counter <- histogram) {
        var unit = HistogramUnit(counter.value, counter.value, counter.count)
        minified = minified :+ unit
      }
    } else {
      var firstIndex = if (histogram(0).value == 0.0) 1 else 0
      if (firstIndex == 1) {
        minified = minified :+ HistogramUnit(histogram(0).value, histogram(0).value, histogram(0).count)
      }
      var firstValue = histogram(firstIndex).value
      var lastValue = histogram(len - 1).value
      var range = lastValue - firstValue
      var decilisRange: Double = 0.0
      var decilisValues: Seq[Double] = Seq()
      if (firstIndex == 0) {
        decilisRange = (range / 10.0).toDouble
        decilisValues = (firstIndex until 10).map(x => (x * decilisRange) + firstValue) :+ lastValue
      } else {
        decilisRange = (range / 9.0).toDouble
        decilisValues = (firstIndex until 10).map(x => ((x-1) * decilisRange) + firstValue) :+ lastValue
      }

      var i = firstIndex
      for (decilisIndex <- 1 until decilisValues.length) {
        var isLast = decilisIndex == decilisValues.length - 1
        var decilis = decilisValues(decilisIndex)
        var cumsum = 0.0
        var value = 0.0
        var count = 0.0
        var hasValueInThisDecilis = false
        var firstValueInDecilis = histogram(i).value
        var lastValueInDecilis = firstValueInDecilis

        while (i <= (histogram.length-1)
          && (isLast || histogram(i).value < decilis)) {
          value = histogram(i).value
          count = histogram(i).count
          cumsum += count
          lastValueInDecilis = value
          hasValueInThisDecilis = true
          i += 1
        }
        if (!hasValueInThisDecilis) {
          firstValueInDecilis = decilisValues(decilisIndex-1)
          lastValueInDecilis = decilis
        }
        var unit = HistogramUnit(firstValueInDecilis, lastValueInDecilis, cumsum)
        minified = minified :+ unit
      }
    }
    minified
  }

}
