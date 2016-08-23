/**
  * Created by UNisar on 8/19/2016.
  */
import org.apache.spark.{SparkConf, SparkContext}

object main {

  /**
    * Creates a spark default context. Ideally, the configuration should be in a configuration file
    * and not hard-coded like this but just trying to get things going
    *
    * @return SparkContext
    */
  def getSparkContext: SparkContext = {
    val conf = new SparkConf()
      .setAppName("Test")
      .setMaster("local[8]")
      .set("spark.executor.memory", "8g")
    val sc = new SparkContext(conf)

    // add the credentials for s3 here. Preferably in a config file :)
    sc
  }

  /**
    * Main method that is responsible for running the show
    *
    * @param args Irrelevant right now
    */

  def main(args: Array[String]) = {
    val sc = getSparkContext
//    val naive = new naiveBayes(sc, "s3n://eds-uga-csci8360/data/project1/X_train_vsmall.txt", "s3n://eds-uga-csci8360/data/project1/y_train_vsmall.txt",
//      "s3n://eds-uga-csci8360/data/project1/X_test_vsmall.txt")

    val naive = new naiveBayes(sc, "D://test.txt", "D://test1.txt", "D://test2.txt")
    naive.train()
    naive.classify()
//    naive.classify("D://test2.txt")
  }
}