import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by gregoire on 23/11/15.
 *
 * TODO test algo en établissant nb < 1 mn decart, < 2mn, <5mn, <10mn <20 mn <30mn et Autres
 */
object Tests {

  /*
   Spark variables
  */
  val nbProc = 1
  val appName = "Testing"

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val conf: SparkConf = new SparkConf().setAppName(appName)
    conf.setMaster("local[" + nbProc + "]")
 
 /** Wrod count */
    val sc: SparkContext = new SparkContext(conf)
    val v = sc.textFile("/home/thales-rt/java_error_in_IDEA_4149.log")

    val ct = v.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _).values.reduce(_ + _)
    println(ct)

    val NUM_SAMPLES = 999999999.0

/** calculate PI */
    val count = sc.parallelize(1 to NUM_SAMPLES.toInt).map { i =>
      val x = Math.random()
      val y = Math.random()
      if (x * x + y * y < 1.0)
        1.0
      else
        0.0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / NUM_SAMPLES)
  }
}
