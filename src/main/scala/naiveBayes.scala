import org.apache.spark.SparkContext

import scala.collection.mutable.HashMap
/**
  * Created by UNisar on 8/20/2016.
  */
class naiveBayes (sc: SparkContext, x: String, y:String, testInput: String) extends java.io.Serializable {

  val documentPerClassType: HashMap[TargetClass, Map[Word, Int]] = new HashMap[TargetClass, Map[Word, Int]]
  val numberOfWordsPerDocument: HashMap[TargetClass, Double] = new HashMap[TargetClass, Double]()
  val probabilities: HashMap[TargetClass, HashMap[Word, Double]] = new HashMap[TargetClass, HashMap[Word, Double]]

  def getScoreForTargetType(words: Array[Word]): TargetClass =  {
    var maxScore = 0
    var target: TargetClass = null
    val results = targetClasses.map ( target => (target, words.map ( word => probabilities(target).getOrElse(word, 0.001)).reduceLeft[Double](_+_)))
    results.maxBy(_._2)._1
  }

  def classify(): Unit =
  {
    testData.foreach ( document => {
      val words = document.split(" ").map(Word(_))
      println(getScoreForTargetType(words).value)
    })
  }

  def train() {
    targetClasses.map ( targetType => documentPerClassType.put(targetType, getSingleDocumentOfTarget(targetType)))
    targetClasses.map ( targetType => new DocumentLength(targetType, documentPerClassType(targetType).size)).map ( x => numberOfWordsPerDocument.put(x.target, x.length))
    targetClasses.foreach (targetType => probabilities(targetType) = new HashMap[Word, Double])

    targetClasses.map ( target => {
      vocabulary.map ( word => {
        val result = getProbabilityOfWordInTarget(word, target)
        probabilities(target).put(word, result)
      })
    })

    documentPerClassType.foreach (w => println(w))
    numberOfWordsPerDocument.foreach (w => println(w))
  }

  def getSingleDocumentOfTarget(target: TargetClass): Map[Word, Int] = data.filter (f => f.labels.contains(target)).flatMap (w => w.wordCount).reduceByKey(_+_).collect().toMap
  def getCountOfWordInDocumentType(word: Word, targetType: TargetClass): Double =  documentPerClassType.getOrElse(targetType, new HashMap[Word, Int]).getOrElse(word, 0).toDouble   // nk
  def getDocumentsOfTarget(target: TargetClass): Double =  data.filter (w => w.labels.contains(target)).count.toDouble               // docsj
  def getClassProbability(target: TargetClass): Double = getDocumentsOfTarget(target)/data.count.toDouble         // Pvj
  def getProbabilityOfWordInTarget(word: Word, targetType: TargetClass): Double = (getCountOfWordInDocumentType(word, targetType)+1.0)/(numberOfWordsPerDocument.getOrElse(targetType, 0.0) + vocabulary.size)  // P(wk|vj)

  // DATA reading stuff
  val termDocsRdd = sc.textFile(x)
  val vocabulary = termDocsRdd.flatMap(y => y.split(" ")).map(Word(_)).distinct().collect()
  val sentinelValue = 1/vocabulary.size
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
}

