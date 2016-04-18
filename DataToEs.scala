/**
  * Created by Hammad  on 3/5/2016.
  */

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.client.Client
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.node.{Node, NodeBuilder}
import org.elasticsearch.spark._
import org.joda.time.DateTime

import scala.collection.immutable.{List => ImmutableList}
import scala.collection.mutable.{HashMap => MutableMap, ListBuffer => MutableList}

object DataToEs {
  val es_node = "localhost"
  val es_port = 9201
  val es_nbShards = 5
  val es_nbReplicas = 1
  val es_cluster_name = "hkust"
  val deleteIfExists = true
  val es_index_name = "notification_all"

  val linemapping = """{
   "notify_data":{
      "properties": {
                    "user" : { "type" : "string", "index":"not_analyzed"},
                    "timestamp_send": {
                        "type": "date",
                        "format": "YYYY/MM/dd HH:mm:ss"
                    },
                    "timestamp_recv": {
                        "type": "date",
                        "format": "YYYY/MM/dd HH:mm:ss"
                    },
                    "weekday": {
                      "type": "string"
                    },
                    "cat_id": {
                      "type": "string"
                    },
                    "article_id": {
                      "type": "string"
                    },
                    "lon": { "type": "string" },
                    "lat": { "type": "string" },
                    "OS" : { "type" : "string", "index":"not_analyzed"},
                    "notification_or_not" : { "type" : "string", "index":"not_analyzed"},
                    "coord":{"type": "geo_point"}
      }
    }
  }"""


  val folder = "D:\\data_spark\\nofication\\"

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
//    System.setProperty("hadoop.home.dir", "D:\\data_spark\\winutils\\")
    val conf: SparkConf = new SparkConf().setAppName("notificationToES")

    conf.setMaster("local[4]")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", es_node)
    conf.set("es.port", es_port.toString)

    initiateIndex

    val sc = new SparkContext(conf)
    val list =  new File(folder).listFiles.sortBy(s=>s.getName)
    for ( f <- list ) {
      println(folder + "\\" + f.getName )
      val Array_data = sc.textFile(folder +"\\" + f.getName)
        .map(_.split(","))
        .map { x => x.map(s => if (s.size > 0) s else "-1") }
        .map(toJson)
        .saveJsonToEs(es_index_name + "/notify_data")
    }
    sc.stop
  }

  //sending time , category id , article id , weekday , receiving_time , lat , long , OS , notification_or_not, userID
  //      0           1              2           3          4            5      6     7            8              9

  def toJson(tab: Array[String]): String = {

    val date_send = new DateTime(tab(0).toLong*1000).toString("YYYY/MM/dd HH:mm:ss")
    val date_recv = new DateTime(tab(4).toLong*1000).toString("YYYY/MM/dd HH:mm:ss")
    var ret = "{"
    ret += "\"user\":\"" + tab(9) + "\","
    ret += "\"timestamp_send\":\"" + date_send + "\","
    ret += "\"timestamp_recv\":\"" + date_recv + "\","
    ret += "\"weekday\":" + tab(3) + ","
    ret += "\"cat_id\":" + tab(1) + ","
    ret += "\"article_id\":" + tab(2) + ","
    ret += "\"lon\":" + tab(6) + ","
    ret += "\"lat\":" + tab(5) + ","
    ret += "\"OS\":" + tab(7) + ","
    ret += "\"notification_or_not\":" + tab(8) + ","
    ret += "\"coord\":[" + tab(6) + "," + tab(5) + "]"
    ret += "}"
    ret
  }

  def initiateIndex: Unit = {
    val esSettings   = Settings.settingsBuilder()
    esSettings.put("number_of_shards", es_nbShards.toString)
    esSettings.put("number_of_replicas", es_nbReplicas.toString)
    val esNode: Node = NodeBuilder.nodeBuilder().clusterName(es_cluster_name)
      .settings(Settings.settingsBuilder().put("http.enabled", false).put("path.home", "C:\\elasticsearch\\data\\")).client(true).node()

    val esClient: Client = esNode.client()

    val indexExists: Boolean = esClient.admin().indices().exists(new IndicesExistsRequest(es_index_name)).actionGet().isExists
    if (indexExists && deleteIfExists) {
      esClient.admin().indices().delete(new DeleteIndexRequest(es_index_name)).actionGet()
      println("--- Index deleted")
      createIndex(esClient, esSettings)
      println("--- Index created")
    }
    else if (!indexExists) {
      createIndex(esClient, esSettings)
      println("--- Index created")
    }
    esNode.close()
  }

  def createIndex(esClient: Client, es_settings: Settings.Builder): Unit = {
    val esCreateIndex: CreateIndexRequest = new CreateIndexRequest(es_index_name)
    esCreateIndex.settings(es_settings)
    esCreateIndex.mapping("notify_data", linemapping)
    esClient.admin().indices().create(esCreateIndex).actionGet()
  }
}
