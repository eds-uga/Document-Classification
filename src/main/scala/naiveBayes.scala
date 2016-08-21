import org.apache.spark.SparkContext

import collection.mutable.HashMap
import scala.collection.mutable

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
    val docmap = new mutable.HashMap[String, Int]
    val wordmap = new mutable.HashMap[String, mutable.HashMap[String, Int]]
    targetClasses.foreach ( x => wordmap(x) = new mutable.HashMap[String, Int])
    entries.foreach ( document => // The document scope
           {
             document._2.intersect(targetClasses).foreach ( target => // The target class scope
             {
               docmap(target) = docmap.getOrElse(target, 0)+1         // Class-Document stats
               document._1.split(" ").foreach ( word =>               // Class-Word-Count stats
               {
                 val map = wordmap(target)
                 map(word) = map.getOrElse(word, 0) + 1
               })
             })
           }
           )

  }
  val xStream = sc.textFile(x)
  val yStream = sc.textFile(y)
  var entries = for ((xs, ys) <- xStream zip yStream) yield new Tuple2(xs, ys.split(",").toSet)
  val targetClasses = Set("CCAT", "ECAT", "GCAT", "MCAT")
}

//case class DocumentCount(document: String, count: Int)
//case class WordCount (word: String, count: Int)
//case class Entry (targetValue: String, counts: Tuple2 [mutable.HashMap[String, Int], Int])