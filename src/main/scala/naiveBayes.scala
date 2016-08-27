import java.util

import com.github.fommil.netlib.BLAS
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.immutable.HashMap
import scala.collection.mutable

/**
  * Created by UNisar on 8/20/2016.
  */
class naiveBayes (sc: SparkContext, x: String, y:String, testInput: String) extends java.io.Serializable {

  /**
    * This method is passed a document as an array of words that runs Naive Bayes algorithm and returns the single target class with the highest score
    *
    * @param words Describes the document as an array of words
    * @return The target class
    */
  def getScoreForTargetType(words: Array[String]): String =  {
    val results = targetClasses.map ( target => (target, classProbability(target) + words.map ( word => getProbabilityOfWordInTarget(word, target)).reduceLeft(_+_)))
    results.maxBy(_._2)._1
  }

  var finalTrainingResult: scala.collection.Map[String, Map[String, Int]] = null
  /**
    * Primarily, this method fills up the following datastructure: finalTrainingResult
    * This is a map(x1) of map(x2)[String, Int], where x1 is the class, x2 is the word and Int is the count
    */
  def train() {
//    finalTrainingResult = finalval.collectAsMap()
  }

  /**
    * This method performs the classification for all the documents and then dump the results in an output file
    */
  def classify(): Unit =
  {
//    testData.map ( document => getScoreForTargetType(document.split("""[\s\W]""").filter (_.length>0).map(_.toLowerCase))).saveAsTextFile("Results")
  }

  //region Helper Methods

  def getCountInTarget (word: String, targetType: String):Double = {
    try {
      return finalTrainingResult(targetType)(word)
    }
    catch {
      case e:Exception => return 0
    }
  }
  /**
    * This method calculates the P (w|v) value where w is a word and v is a target value
    *
    * @param word Word that we need the probability for
    * @param targetType Target class that we are interested in
    * @return Returns a double value
    */
  def getProbabilityOfWordInTarget(word: String, targetType: String): Double = {
    val value = math.log10((getCountInTarget(word, targetType) + 1) / (finalTrainingResult(targetType).size + vocabularySize)) // P(wk|vj)
    value
  }
  //endregion

  //region DATA reading stuff, pretty straightforward

  val termDocsRdd = sc.textFile(x)
  //  val vocabulary = termDocsRdd.flatMap(y => y.split(" ")).map(Word(_)).distinct().collect()
  val vocabularySize = termDocsRdd.flatMap(y => y.split(" ")).distinct().count()
  val sentinelValue = 1.0/vocabularySize
  val xStream = sc.textFile(x)
  val yStream = sc.textFile(y)
  val testStream = sc.textFile(testInput)
  var testData = for (xs <- testStream) yield xs

  val first = xStream.zipWithIndex().map(x => (x._2, x._1))
  val second = yStream.zipWithIndex().map ( x => (x._2, x._1))
  val finalout = first.cogroup(second).map ( x => (x._2._1, x._2._2))
  val finalStream =  for (x <- finalout)  yield new Tuple2(x._2.head, x._1.head)

  val targetClasses = Set("CCAT", "ECAT", "GCAT", "MCAT")
  val data:RDD[(String, Vector[String])] = finalStream.flatMap ( x =>
  {
    val targetTypes =  x._1.split(",").toSet.intersect(targetClasses)
    if (targetTypes.isEmpty)
      None
    else
      targetTypes.map ( b => (b, x._2.split(" ").toVector))
  })

  def getMap(vals: Vector[String]): mutable.HashMap[String, Int] = {
    val map = new mutable.HashMap[String, Int]()
    vals.foreach ( v => map.put(v, 1))
    map
  }

  def mergeMaps(output: mutable.HashMap[String, Int], input: Vector[String]): mutable.HashMap[String, Int] = {
    input.foreach ( v => output.put(v, output.getOrElse(v, 0) + 1))
    output
  }

  def createCombiner(vals: Vector[String]): (Long, mutable.HashMap[String, Int]) = {
    (1L, getMap(vals))
  }

  def mergeValue(c: (Long, mutable.HashMap[String, Int]), v: Vector[String]): (Long, mutable.HashMap[String, Int])  = {
    (c._1 + 1L, mergeMaps(c._2, v))
  }


  def mergeMaps1(first: mutable.HashMap[String, Int], second: mutable.HashMap[String, Int]) = {
    second.foreach ( x => {
      if (first.contains(x._1))
        first.put(x._1, first(x._1) + x._2)
      else
        first.put(x._1, x._2)
    })
  }

  def mergeCombiners(c1: (Long,  mutable.HashMap[String, Int]), c2: (Long, mutable.HashMap[String, Int])) = {
    (c1._1 + c2._1, mergeMaps1(c1._2, c2._2))
  }
  val aggregated = data.map ( x => (x._1, x._2)).combineByKey(createCombiner, mergeValue, mergeCombiners)

//  aggregated.foreach (x => println(x._1 + " " + x._2._1 + " " + x._2._2))

//    mergeValue = (c: (Long, DenseVector), v: Vector) => {
//      BLAS.axpy(1.0, v, c._2)
//      (c._1 + 1L, c._2)
//    },
//    mergeCombiners = (c1: (Long, DenseVector), c2: (Long, DenseVector)) => {
//      BLAS.axpy(1.0, c2._2, c1._2)
//      (c1._1 + c2._1, c1._2)
//    }
//  ).collect().sortBy(_._1)

//  val finalval = data.filter (x => targetClasses.contains(x._1)).map ( x => (x._1, x._2.split("""[\s\W]""").filter (_.length > 0).map (_.toLowerCase).foldLeft(Map.empty[String, Int]) {
//    (count, word) => count + (word -> (count.getOrElse(word, 0) + 1))
//  }))

  var temp = yStream.flatMap(x => x.split(",")).filter ( x => targetClasses.contains(x)).countByValue()
  val totalCount = temp.map (x => x._2).sum
  val classProbability = temp.map ( x => (x._1, math.log10(x._2.toDouble/totalCount)))
  //endregion
}


