import org.joda.time.DateTime
import com.github.nscala_time.time.Imports._

case class NotificationRecord(sending_time : DateTime , category_id : String, article_id: String , weekday : String, receiving_time: DateTime , lat : String, long: String , OS : String, notification_or_not: String, userID : String ) {
   def customString(): String ={
    Array[String](userID, sending_time.getMillis.toString ,category_id).mkString(";")
  }

  def customString2(): String ={
    Array[String]( sending_time.getMillis.toString , category_id).mkString(";")
  }
}

object NotificationObject {
      def toNotification(array: Array[String]): NotificationRecord = {

        val send_time = if(array(0).length > 11 ) array(0).toLong else array(0).toLong*1000
        val rec_time  = if(array(0).length > 11 ) array(0).toLong else array(0).toLong*1000

        var sending_time   = new DateTime(send_time)
        var receiving_time = new DateTime(rec_time)

        val sending_sec = sending_time.getSecondOfDay / 900
        val recv_sec    = receiving_time.getSecondOfDay / 900

        sending_time   = new DateTime(sending_time.getMillis    - sending_time.getSecondOfDay*1000   + sending_sec*900*1000)
        receiving_time = new DateTime(receiving_time.getMillis  - receiving_time.getSecondOfDay*1000 + recv_sec *900*1000)

        NotificationRecord(sending_time, array(1),array(2),array(3),receiving_time,array(5),array(6),array(7),array(8),array(9))
      }

}