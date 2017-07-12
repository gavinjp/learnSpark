package chapter4

import java.util.Random

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2017/7/11.
  */
object UserTopicDemo {
  val topicList = List("A", "B", "C", "D", "E")
  val random = new Random()

  //生成用户数据
  def produceUserData(num: Int): ListBuffer[(String, UserInfo)] = {
    val userList = new ListBuffer[(String, UserInfo)]

    for (i <- 1 to num) {
      val topics = mutable.Set[String]()
      for (i <- 1 to 3) {
        val topic = topicList(random.nextInt(topicList.length))
        topics += topic
      }
      val pair = ("gavin" + i, UserInfo(topics))
      userList += pair
    }
    println(s"=================用户订阅信息=================")
    userList.foreach(println)
    userList
  }

  //生成用户浏览数据
  def produceLinks(userId: String, num: Int): ListBuffer[(String, LinkInfo)] = {
    val linkList = new ListBuffer[(String, LinkInfo)]
    for(i <- 1 to num) {
      val topic = topicList(random.nextInt(topicList.length))
      val link = (userId, LinkInfo(topic))
      linkList += link
    }
    println(s"=================用户【$userId】浏览数据=================")
    linkList.foreach(println)
    linkList
  }

  def processNewLogs(events: RDD[(String, LinkInfo)], userData: RDD[(String, UserInfo)]): Unit = {

    //内连接
    val joined = userData.join(events)
    println(s"=================用户订阅主题和浏览信息内连接=================")
    joined.foreach(println)
    val offTopic = joined.filter {
      case (userId, (userInfo, linkInfo)) => !userInfo.topics.contains(linkInfo.topic)
    }
    println("=================用户浏览的未订阅主题=================")
    offTopic.foreach(println)
    println("用户浏览的未订阅主题（未去重）: " + offTopic.count())

    //去重
    val offTopicDistinct = offTopic.distinct()
    println("=================用户浏览的未订阅主题去重后=================")
    offTopicDistinct.foreach(println)
    val offTopicVisits = offTopicDistinct.count()
    println("Number of Visits to non-subscribed topics: " + offTopicVisits)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("userTopic")
    val sc = new SparkContext(conf)

    //用户总数量
    val userNum = 100
    val linksList = new ListBuffer[(String, LinkInfo)]

    //过去五分钟内访问的用户数量（随机生成）
    val currentUserNum = random.nextInt(100) + 1

    //用户订阅的主题
    /*val pair1 = ("gavin1", UserInfo(mutable.Set("A", "B", "C")))
    val pair2 = ("gavin2", UserInfo(mutable.Set("B", "C", "D")))
    val userData = sc.parallelize(List(pair1, pair2))*/
    val userList = produceUserData(userNum)
    val userData = sc.parallelize(userList)

    //用户浏览数据
    /*val linkList1 = produceLinks("gavin1", 8)
    val linkList2 = produceLinks("gavin2", 10)
    val linkList = linkList1 ++ linkList2
    val events = sc.parallelize(linkList)*/

    println("=================此次五分钟内共有" + currentUserNum + "名用户浏览主题=================")
    for (i <- 1 to currentUserNum) {
      val num = random.nextInt(100) + 1
      linksList ++= produceLinks("gavin" + num, random.nextInt(10) + 1)
    }
    val events = sc.parallelize(linksList)

    //周期性调用函数来处理过去五分钟产生的事件日志
    processNewLogs(events, userData)
  }
}

case class UserInfo(topics: mutable.Set[String])
case class LinkInfo(topic: String)