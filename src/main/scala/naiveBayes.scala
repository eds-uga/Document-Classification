import org.apache.spark.SparkContext
import scala.collection.mutable.HashMap

/**
  * Created by UNisar on 8/20/2016.
  */
class naiveBayes (sc: SparkContext, x: String, y:String, testInput: String) extends java.io.Serializable {

  //region Private members

  val documentPerClassType: HashMap[TargetClass, Map[Word, Int]] = new HashMap[TargetClass, Map[Word, Int]]
  val numberOfWordsPerDocument: HashMap[TargetClass, Double] = new HashMap[TargetClass, Double]()
  val probabilities: HashMap[TargetClass, HashMap[Word, Double]] = new HashMap[TargetClass, HashMap[Word, Double]]
  val classProbabilities: HashMap[TargetClass, Double] = new HashMap[TargetClass, Double]
  //endregion

  /**
    * This method is passed a document as an array of words that runs Naive Bayes algorithm and returns the single target class with the highest score
    * @param words Describes the document as an array of words
    * @return The target class
    */
  def getScoreForTargetType(words: Array[Word]): TargetClass =  {
    val results = targetClasses.map ( target => (target, words.map ( word => probabilities(target).getOrElse(word, sentinelValue)).reduceLeft[Double](_+_)))
    results.maxBy(_._2)._1
  }

  /**
    * This method performs the classification for all the documents and then dump the results in an output file
    */
  def classify(): Unit =
  {
    testData.foreach ( document => {
      val words = document.split(" ").map(Word(_))
      println(getScoreForTargetType(words).value)
    })
  }

  /**
    * Primarily, this method fills up the following datastructure: HashMap [TargetClass, HashMap [Word, Double]] which essentially depicts the P(word|class) value.
    * For every possible target value, we have the pre-calculated probabilities for all the words in the context of active corpus.
    * In addition, it also fills up the following map: HashMap [TargetClass, Double] where we store the distinct count of words for every class type
    */
  def train() {
    targetClasses.map ( targetType => documentPerClassType.put(targetType, getSingleDocumentOfTarget(targetType)))
    targetClasses.map ( targetType => new DocumentLength(targetType, documentPerClassType(targetType).size)).map ( x => numberOfWordsPerDocument.put(x.target, x.length))
    targetClasses.foreach (targetType => probabilities(targetType) = new HashMap[Word, Double])
    targetClasses.foreach ( target => classProbabilities.put(target, getClassProbability(target)))

    targetClasses.map ( target => {
      vocabulary.map ( word => {
        val result = getProbabilityOfWordInTarget(word, target)
        probabilities(target).put(word, result)
      })
    })
  }

  //region Helper Methods
  /**
    * Returns a single document against the passed target value as a Map [Word, Int] where essentially it is a key-value pair. Key is word and Value is the number of occurrences
    * @param target The target value that we need the document for
    * @return A hashmap where the key is Word and Value is the number of occurrences for the given Word
    */
  def getSingleDocumentOfTarget(target: TargetClass): Map[Word, Int] = data.filter (f => f.labels.contains(target)).flatMap (w => w.wordCount).reduceByKey(_+_).collect().toMap

  /**
    * Returns the number of occurrences of a given word in the document of the given target value
    * @param word The word that we need the count of
    * @param targetType The target class that we need the count against
    * @return Returns the count as a double value, if not found returns zero as default
    */
  def getCountOfWordInDocumentType(word: Word, targetType: TargetClass): Double =  documentPerClassType.getOrElse(targetType, new HashMap[Word, Int]).getOrElse(word, 0).toDouble   // nk

  /**
    * Returns the count of records that have target as the class type
    * @param target Target value that we are interested in
    * @return Returns the count of documents that are classified as target
    */
  def getDocumentsOfTarget(target: TargetClass): Double =  data.filter (w => w.labels.contains(target)).count.toDouble               // docsj

  /**
    * Returns the probability of finding a given class based on current corpus. Does so with a simple division operation
    * @param target
    * @return
    */
  def getClassProbability(target: TargetClass): Double = getDocumentsOfTarget(target)/data.count.toDouble         // Pvj

  /**
    * This method calculates the P (w|v) value where w is a word and v is a target value
    * @param word Word that we need the probability for
    * @param targetType Target class that we are interested in
    * @return Returns a double value
    */
  def getProbabilityOfWordInTarget(word: Word, targetType: TargetClass): Double = (getCountOfWordInDocumentType(word, targetType)+1.0)/(numberOfWordsPerDocument.getOrElse(targetType, 0.0) + vocabulary.size)  // P(wk|vj)
  //endregion


  //region DATA reading stuff, pretty straightforward

  val termDocsRdd = sc.textFile(x)
  val vocabulary = termDocsRdd.flatMap(y => y.split(" ")).map(Word(_)).distinct().collect()
  val sentinelValue = 1.0/vocabulary.size.toDouble
  val xStream = sc.textFile(x)
  val yStream = sc.textFile(y)
  val testStream = sc.textFile(testInput)
  var testData = for (xs <- testStream) yield xs
  var rawData = for ((xs, ys) <- xStream zip yStream)  yield new Tuple2(xs, ys)
  val targetClasses = Set(TargetClass("CCAT"), TargetClass("ECAT"), TargetClass("GCAT"), TargetClass("MCAT"))
  val data = rawData.flatMap { line =>
    var targetTypes = line._2.split(",").map(TargetClass(_)).toSet
    if (targetTypes.intersect(targetClasses).isEmpty)
      None
    else
    {
      try
      {
        val wordCount = line._1.split(" ").map(Word(_)).foldLeft(Map.empty[Word, Int]) {
          (count, word) => count + (word -> (count.getOrElse(word, 0) + 1))
        }
        Some(Document(wordCount, targetTypes.intersect(targetClasses)))
      } catch {
        case e: NumberFormatException => None
      }
    }
  }.cache()
  //endregion
}

