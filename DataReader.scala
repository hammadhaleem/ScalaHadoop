
import com.github.nscala_time.time.Imports._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

import scala.collection.mutable.{HashMap => MutableMap, ListBuffer => MutableList}


object DataReader{
  def main(args: Array[String]): Unit = {
    val folder :String = "/Users/hammadhaleem/social/data/"
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf: SparkConf = new SparkConf().setAppName("notificationToES")

    conf.setMaster("local[8]")


    val sc = new SparkContext(conf)


    val dataLoaded : RDD[NotificationRecord] = sc.textFile(folder)
      .map(_.split(","))
      .map { x => x.map(s => if (s.size > 0) s else "-1") }
      .map(item => NotificationObject.toNotification(item))
    val mid_time : Long = 1432879200.toLong *1000.toLong // partition at Thu, 28 May 2015 06:00:00

    val test = dataLoaded.filter{
      case e : NotificationRecord => e.sending_time.getMillis > mid_time
    } .map(x=> (x.userID,x.customString2)).groupByKey()
      .map(s=>s._1 +"," + s._2.mkString(","))
      .saveAsTextFile("/Users/hammadhaleem/social/output/test/")


    println("Test")

    val train = dataLoaded.filter {
      case e: NotificationRecord =>  e.sending_time.getMillis < mid_time
    }
      .map(x=> (x.userID , x.customString2)).groupByKey()
      .map(s=>s._1 +"," + s._2.mkString(","))
      .saveAsTextFile("/Users/hammadhaleem/social/output/train/")

    println("Train")


    dataLoaded
      .map(x=>(x.userID , 1))
      .reduceByKey((x,y) => x +y )
      .filter(k=>k._2 > 200)
      .sortBy(k=>k._2)
      .saveAsTextFile("/Users/hammadhaleem/social/output/users/")

    sc.stop
  }
}