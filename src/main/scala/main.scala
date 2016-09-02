/**
  * Created by UNisar on 8/19/2016.
  */
import org.apache.spark.{SparkConf, SparkContext}

object main {

  /**
    * Creates a spark default context. Ideally, the configuration should be in a configuration file
    * and not hard-coded like this but just trying to get things going
    * @return SparkContext
    */
  def getSparkContext: SparkContext = {
    val conf = new SparkConf()
      .setAppName("Test")
      .setMaster("local[4]")
      .set("spark.executor.memory", "14g")
    val sc = new SparkContext(conf)
    sc
  }

  /**
    * Main method that is responsible for running the show
    * @param args Irrelevant right now
    */

  def main(args: Array[String]) = {
    val sc = getSparkContext
    try
      val naive = new naiveBayes(sc, args{0}, args{1}, args{2}, false)
      naive.train()
      naive.classify()
    catch {
      case e: Exception => throw e
    }
  }
}