package chapter6

import org.apache.spark.{SparkContext, SparkConf}

import scala.io.Source

/**
  * Created by Administrator on 2017/8/3.
  * 查询RDD contactCounts中的呼号的对应位置。
  * 将呼号前缀读取为国家代码来进行查询。
  */
object BroadcastDemo2 {
  def loadCallSignTable() = {
    val input = Source.fromFile("E:/first/callsign_tbl").getLines().toList
    val tuples = input.map(line => {
      val arr = line.split(",")
      (arr(0).trim, arr(1).trim)
    })
    tuples
  }
  def lookupInArray(sign: String, signPrefixesValues: List[(String, String)]) = {
    var result = ""
    signPrefixesValues.foreach(f => {
      val prefix = f._1
      val country = f._2
      if (sign.equals(prefix)) {
//        println(country)
        result = country
      }
    })
    result
  }
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("BroadcastDemo")
    val sc = new SparkContext(conf)
    val signPrefixes = sc.broadcast(loadCallSignTable())
    val contactCounts = sc.parallelize(List(("ALZ", 5), ("COZ", 16), ("ELZ", 19),
      ("ALZ", 4), ("COZ", 4)))
    val contryContactCounts = contactCounts.map{
      case (sign, count) =>
        val country = lookupInArray(sign, signPrefixes.value)
        (country, count)
    }.reduceByKey((x, y) => x + y)
    contryContactCounts.saveAsTextFile("E:/first/countries")
  }
}
