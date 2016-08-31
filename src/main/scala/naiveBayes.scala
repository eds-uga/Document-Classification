import java.io.PrintWriter

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.{Map, mutable}

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
  def getScoreForTargetType(words: Array[String]) = targetClasses.map(target => (target, (classProbability.get(target) + words.map(word => getProbabilityOfWordInTarget(word, target)).reduceLeft(_ + _)))).maxBy(_._2)._1

  /**
    * Inefficient method get the length of a given class
    *
    * @param targetType
    * @return
    */
  def getTotalWords(targetType: String): Double = {
    var total = 0.0
    for ((k, v) <- aggregated(targetType)._2)
      total += v
    total
  }

  /**
    * Primarily, this method fills up the following datastructure: finalTrainingResult
    * This is a map(x1) of map(x2)[String, Int], where x1 is the class, x2 is the word and Int is the count
    */
  def train() {
    aggregated = corpusIterator.map(x => (x._1, x._2)).combineByKey(createCombiner, mergeValue, mergeCombiners).collectAsMap
    var numberOfDocuments = 0L
    aggregated.foreach(numberOfDocuments += _._2._1)
    // normalizing all the values
    normalizationValues = targetClasses.map(x => (x, aggregated(x)._2.maxBy(b => b._2)._2)).toMap
    totalWordsPerClass = targetClasses.map(x => (x, getTotalWords(x))).toMap
    targetClasses.foreach(target => classProbability.put(target, (aggregated(target)._1.toDouble / numberOfDocuments)))
    getTfIdf();
  }

  /**
    * This method performs the classification for all the documents and then dump the results in an output file
    */
  def classify(): Unit = {
    val results = testData.map(document => {
      getScoreForTargetType(document.split(tokenizer).filter(_.length > 0).filter(c => !stopWordList.contains(c)).map(_.toLowerCase).toSet.intersect(vocabulary).toArray)
    }).coalesce(1).saveAsTextFile("/Users/yangfan/Documents/shannondata/resultone")
  }

  def getCountInTarget(word: String, targetType: String): Double = {
    try {
      return aggregated(targetType)._2(word)
    }
    catch {
      case e: Exception => return 0
    }
  }

  /**
    * Returns the total occurrences of a given word in the whole corpus
    *
    * @param word
    * @return
    */
  def totalCounts(word: String): Double = {
    var total = 0.0
    for (t <- targetClasses)
      total += getCountInTarget(word, t)
    total
  }

  /**
    * This method calculates the P (w|v) value where w is a word and v is a target value
    *
    * @param word       Word that we need the probability for
    * @param targetType Target class that we are interested in
    * @return Returns a double value
    */
  def getProbabilityOfWordInTarget(word: String, targetType: String) = math.log10((getCountInTarget(word, targetType) + 1) / (totalWordsPerClass(targetType) + vocabulary.size)) // P(wk|vj)

  /**
    * Helper method to determine if the passed string is a number
    *
    * @param text
    * @return
    */
  def isAllDigits(text: String) = text forall Character.isDigit

  /**
    * This method calculates the every term in vocabulary and store in TFIDP which is in form of(TARGET,TERM,TFIDF)
    */

  var TFIDF: Set[(String, String, Double)] =null
  def getTfIdf(): Unit = {
    val number0fDocsInLibs = documents.count
    TFIDF = for (T <- targetClasses; v <- vocabulary) yield (T, v, getProbabilityOfWordInTarget(v, T) + math.log10(number0fDocsInLibs) - math.log10(documents.filter(x => x._2.contains(v)).count() + 1))

  }

  // TFIDF clssifier
  def classify2(): Unit = {
    val results = testData.map(document => {
      getScoreForTargetType(document.split(tokenizer).filter(_.length > 0).filter(c => !stopWordList.contains(c)).map(_.toLowerCase).toSet.intersect(vocabulary).toArray)
    }).coalesce(1).saveAsTextFile("/Users/yangfan/Documents/shannondata/result")
  }
  // get score for a doc in TFIDF way

  /*
   *   get weighted TfIdf for a specific Word in specific Target
   */
  def getSinglegetTfIdf(Target1:String,Word1:String):Double =
  {  val SpecificTfIdf = if(TFIDF.filter(x=>x._1==Target1).filter(x=>x._2==Word1).isEmpty)  0
  else TFIDF.filter(x=>x._1==Target1).filter(x=>x._2==Word1).map(x=>x._3).head
    SpecificTfIdf
  }

  def getScoreForTargetType2(words: Array[String])
  = targetClasses.map(target => (target, (classProbability.get(target) + words.map(word => getSinglegetTfIdfPos(target,word)).reduceLeft(_ + _)))).maxBy(_._2)._1

  /*
 *   get weighted TfIdf  Posibility for a specific Word in specific Target P(TfIdf/ weighted # of words in Target)
 */
  def getSinglegetTfIdfPos(targetType: String,word: String) :Double = (getSinglegetTfIdf(targetType,word)+1) / (getTotalWords2(targetType)) //+ vocabulary.size) // P(wk|vj)
  // total weights of words in Target Doc in Training

  /*  weighted # of words in Target
   *
   */
  def getTotalWords2(targetType: String): Double = {
    val maptemp:Map[String,Double]= aggregated.get(targetType).get._2
    val total = {for((k,v)<-maptemp)
      yield {(maptemp.get(k).get) * getSinglegetTfIdf(targetType,k)}}.foldLeft(0:Double){(m:Double,n:Double) => m+n}
    total
  }


  //region DATA reading stuff, pretty straightforward
  val tokenizer = """[\s\W]"""  // The tokenizer to split the strings, TODO: improve this
  val classProbability = new java.util.HashMap[String, Double] // This structure holds the mapping from CLASS -> P(V)
  var normalizationValues: Map[String, Double] = null
  var totalWordsPerClass: Map[String, Double] = null
  val stopWordList = sc.textFile("StopWordList.txt").collect.toSet
  var vocabulary = sc.textFile(x).flatMap(x => x.split(tokenizer)).filter(_.length > 0).map(_.toLowerCase()).distinct.collect.
    filter(!isAllDigits(_)).filter(c => !stopWordList.contains(c)).toSet
  sc.broadcast(vocabulary)

  var aggregated: Map[String, (Long, Map[String, Double])] = null
  var testData =  sc.textFile(testInput)
  val documents = sc.textFile(x).zipWithIndex().map(x => (x._2, x._1))
  val labels = sc.textFile(y).zipWithIndex().map(x => (x._2, x._1))
  val merger = documents.cogroup(labels).map(x => (x._2._1, x._2._2))
  val mergeIterator = for (x <- merger) yield new Tuple2(x._2.head, x._1.head)
  val targetClasses = Set("CCAT", "ECAT", "GCAT", "MCAT")
  val corpusIterator: RDD[(String, Vector[String])] = mergeIterator.flatMap(x => {
    val targetTypes = x._1.split(",").toSet.intersect(targetClasses)
    if (targetTypes.isEmpty)
      None
    else
      targetTypes.map(b => (b, x._2.split(tokenizer).filter(_.length > 0).map(_.toLowerCase).filter ( f => vocabulary.contains(f)).toVector))
  })


  /**
    * Creates a map for the passed strings
    *
    * @param vals
    * @return
    */
  def getMap(vals: Vector[String]): mutable.Map[String, Double] = {
    var map = mutable.Map[String, Double]()
    for (i <- vals) {map.put(i,map.getOrElse(i,0:Double)+1)}
    map
  }

  def mergeMaps(output: mutable.Map[String, Double], input: Vector[String]) = input.foreach(x => output.put(x, output.getOrElse(x, 0.toDouble) + 1))

  /**
    * Creates a combiner against a new key for the main combineByKey method
    *
    * @param values
    * @return
    */
  def createCombiner(values: Vector[String]): (Long, mutable.Map[String, Double]) = (1, getMap(values))

  /**
    * This method merges an entry against an existing Combiner
    *
    * @param combiner
    * @param values
    * @return
    */
  def mergeValue(combiner: (Long, mutable.Map[String, Double]), values: Vector[String]) = {
    mergeMaps(combiner._2, values)
    (combiner._1 + 1, combiner._2)
  }

  /**
    * This method merges two existing combiners
    *
    * @param firstCombiner
    * @param secondCombiner
    * @return
    */
  def mergeCombiners(firstCombiner: (Long, mutable.Map[String, Double]), secondCombiner: (Long, mutable.Map[String, Double])) = {
    mergeMaps(firstCombiner._2, secondCombiner._2)
    (firstCombiner._1 + secondCombiner._1, firstCombiner._2)
  }

  def mergeMaps(output: mutable.Map[String, Double], input: mutable.Map[String, Double]) = for ((k,v) <- input) output.put(k, output.getOrElse(k, 0.toDouble) + v)

}