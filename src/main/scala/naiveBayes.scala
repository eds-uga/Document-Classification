import org.apache.spark.SparkContext

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
    val results =
      targetClasses.map ( target => (target, classProbability(target) +
        words.map ( word => getProbabilityOfWordInTarget(word, target)).reduceLeft(_+_)))
    results.maxBy(_._2)._1
  }

  var finalTrainingResult: scala.collection.Map[String, Map[String, Int]] = null
  /**
    * Primarily, this method fills up the following datastructure: finalTrainingResult
    * This is a map(x1) of map(x2)[String, Int], where x1 is the class, x2 is the word and Int is the count
    */
  def train() {
      finalTrainingResult = finalval.collectAsMap()
  }

  /**
    * This method performs the classification for all the documents and then dump the results in an output file
    */
  def classify(): Unit =
  {
    testData.map ( document => getScoreForTargetType(document.split("""[\s\W]""").filter (_.length>0).map(_.toLowerCase))).coalesce(1,true).saveAsTextFile("/Users/yangfan/Documents/shannondata/results")
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

  /**
    * This method calculates the every term in vocabulary and store in TFIDP which is in form of(TARGET,TERM,TFIDF)
    */
  //tf-idf
  def getTFIDF():Unit ={
//    val IFij = getProbabilityOfWordInTarget(word,targetType)
    val number0fDocsInLibs = first.count()
//    val numberOfDocsInLibsHasWord = first.filter( x=> x._2.contains(word)).count()
//    val IDFi =math.log10( number0fDocsInLibs) -math.log10 (number0fDocsInLibs +1)
    val TFIDF = targetClasses.map(T=> vocabulary.map( v=> (T,v,getProbabilityOfWordInTarget(v,T) + math.log10( number0fDocsInLibs) -math.log10 (first.filter( x=> x._2.contains(v)).count() +1)  ) ))
    //println("DEBUGTFIDF"+TFIDF)

  }






  //region DATA reading stuff, pretty straightforward

  val termDocsRdd = sc.textFile(x)
  val vocabulary = termDocsRdd.flatMap(y => y.split("\\s+")).distinct().collect()
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
  val data = finalStream.flatMap ( x =>
  {
    val targetTypes = x._1.split(",")
    if (targetTypes.toSet.intersect(targetClasses).isEmpty)
      None
    else
      targetTypes.map ( b => (b, x._2))
  }).reduceByKey(_+ " " +_)

  val finalval = data.filter (x => targetClasses.contains(x._1)).map ( x => (x._1, x._2.split("""[\s\W]""").filter (_.length > 0).map (_.toLowerCase).foldLeft(Map.empty[String, Int]) {
    (count, word) => count + (word -> (count.getOrElse(word, 0) + 1))
  }))

  var temp = yStream.flatMap(x => x.split(",")).filter ( x => targetClasses.contains(x)).countByValue()
  val totalCount = temp.map (x => x._2).sum
  val classProbability = temp.map ( x => (x._1, math.log10(x._2.toDouble/totalCount)))
  //endregion
}

