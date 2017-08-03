package chapter6

import org.apache.spark.{SparkContext, SparkConf}

import scala.io.Source

/**
  * Created by Administrator on 2017/8/3.
  * 查询RDD contactCounts中的呼号的对应位置。
  * 将呼号前缀读取为国家代码来进行查询。
  */
object BroadcastDemo1 {
  def loadCallSignTable() = {
    val input = Source.fromFile("E:/first/callsign_tbl").getLines().toList
    val tuples = input.map(line => {
      val arr = line.split(",")
      (arr(0).trim, arr(1).trim)
    })
    tuples
  }
  def lookupCountry(sign: String, signPrefixes: List[(String, String)]) = {
    var result = ""
    signPrefixes.foreach(f => {
      val prefix = f._1
      val country = f._2
      if (sign.equals(prefix)) {
//        println(country)
        result = country
      }
    })
    result
  }
  def processSignCount(sign_count: (String, Int),
                       signPrefixes: List[(String, String)]) = {
    val country = lookupCountry(sign_count._1, signPrefixes)
    val count = sign_count._2
    (country, count)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("BroadcastDemo")
    val sc = new SparkContext(conf)
    val contactCounts = sc.parallelize(List(("ALZ", 5), ("COZ", 16), ("ELZ", 19),
      ("ALZ", 4), ("COZ", 4)))
    val signPrefixes = loadCallSignTable()
    val countryContactCounts = contactCounts.map(processSignCount(_, signPrefixes))
      .reduceByKey((x, y) => x + y)
    countryContactCounts.foreach(println)
  }
}
