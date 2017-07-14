package chapter4

import java.util.Random

import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2017/7/14.
  */
object PageRankDemo {
  val pages = List("A", "B", "C", "D", "E")
  val random = new Random()
  def producePageLinks: mutable.HashMap[String, ListBuffer[String]] = {
    val pageLinks = new mutable.HashMap[String, ListBuffer[String]]()
    //    val pl1 = ("A", List("B", "C"))
    for (i <- 0 to pages.length - 1) {
      val linkList = new ListBuffer[String]
      for (j <- 0 to random.nextInt(pages.length)) {
        linkList += pages(random.nextInt(pages.length))
      }
      pageLinks += ((pages(i), linkList.distinct))
    }
    pageLinks.foreach(println)
    pageLinks
  }
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("PageRank")
    val sc = new SparkContext(conf)
    val links = sc.parallelize(producePageLinks.toList)
    /*val links = sc.parallelize(producePageLinks.toList)
                   .partitionBy(new HashPartitioner(100))
                   .persist()*/

    //将每个页面的排序值初始化为1.0；由于使用mapValues，
    //生成的RDD的分区方式会和"links"的一样
    var ranks = links.mapValues(v => 1.0)
    ranks.foreach(println)

    //运行10轮PageRank迭代
    for (i <- 0 until 10) {
      val joined = links.join(ranks)
      println("======================joined======================")
      joined.foreach(println)
      val contributions = joined.flatMap {
        case (pageId, (linkList, rank)) =>
          linkList.map(dest => (dest, rank / linkList.size))
      }
      println("======================contributions======================")
      contributions.foreach(println)
      ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85 * v)
      println("======================ranks======================")
      ranks.foreach(println)
    }
//    ranks.saveAsTextFile("ranks")
  }
}