import org.apache.spark._
import org.apache.spark.SparkContext._
import scala.collection.mutable._
/**
  * Created by UNisar on 8/20/2016.
  */
class naiveBayes (sc: SparkContext, x: String, y:String ) extends java.io.Serializable {

  /**
    * This function iterates through x and y, and builds the necessary frequencies
    * so we can train the Naive Bayes classifier
    */
  def train() {
    //
    val docmap = new HashMap[String, Int]
    val wordmap = new HashMap[String, HashMap[String, Int]]
    targetClasses.foreach ( x => wordmap(x) = new HashMap[String, Int])
    var docsj = yStream.flatMap( line => line.split(",").toSet.intersect(targetClasses).map(word => (word, 1.0))).reduceByKey(_+_)
    docsj.foreach ( y => println (y))
    var totalDocs = yStream.count()
    val Pv = docsj.map( x => (x._2/totalDocs))
    Pv.foreach ( x => println (x))

//    val groupedDocuments = entries.flatMap ( line => line._1.map ( x => (x, line._2)))
//    groupedDocuments.foreach ( u => println (u._1) + " " + println (u._2))

    entries.foreach ( u => println (u))
//    val wordsCount = groupedDocuments.map ( x => x.)
//    println (totalDocuments)
  }

  val xStream = sc.textFile(x)
  val totalDocuments = xStream.count()
  val yStream = sc.textFile(y)


//  var entries = for ((xs, ys) <- xStream zip yStream)  yield new Tuple2(xs, ys.split(",").toSet.intersect(targetClasses))
  var entries = for ((xs, ys) <- xStream zip yStream)  yield new Tuple2(ys.split(",").toSet.intersect(targetClasses), xs.split(" ").toList.map(w => (w,1.0)))
  val targetClasses = Set("CCAT", "ECAT", "GCAT", "MCAT")
}

case class DocumentCount(target: String, count: Int)
//case class WordCount (word: String, count: Int)
//case class Entry (targetValue: String, counts: Tuple2 [mutable.HashMap[String, Int], Int])