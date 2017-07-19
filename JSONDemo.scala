package chapter5

import com.google.gson.{JsonElement, Gson}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2017/7/18.
  */

object JSONDemo {
  val gson = new Gson()
//  def readJson2Object(input: RDD[String])(classFullName: String)(implicit clazz: Class[_] = Class.forName(classFullName)) = {
  def readJson2Object(input: RDD[String])(clazz: Class[_]) = {
    input.flatMap(record => {
      try {
        Some(gson.fromJson(record, clazz))
      } catch {
        case e: Exception => None
      }
    })
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("JSONDemo")
    val sc = new SparkContext(conf)
    val input = sc.textFile("file:///E:/first/second/pandainfo.json")

    /*val result = input.flatMap(record => {
      try {
        Some(new Gson().fromJson(record, classOf[Person]))
      } catch {
        case e: Exception => None
      }
    })*/
//    val result = readJson2Object(input)("chapter5.Person")
    val result = readJson2Object(input)(classOf[Person])
    println("==================result====================")
    result.foreach(println)

    result.filter(p => p.asInstanceOf[Person].lovesPandas)
      .map(gson.toJson(_)).saveAsTextFile("file:///E:/first/second/lovesPandas")
  }
}

case class Person(name: String, lovesPandas: Boolean) {
  override def toString: String = "name: " + name + "\tlovesPandas: " + lovesPandas
}
