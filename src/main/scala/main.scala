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
      .setMaster("local")
      .set("spark.executor.memory", "4g")
    new SparkContext(conf)
  }

  /**
    * Main method that is responsible for running the show
    * @param args Irrelevant right now
    */

  def main(args: Array[String]) = {
    val sc = getSparkContext
    println(sc.sparkUser)
  }
}