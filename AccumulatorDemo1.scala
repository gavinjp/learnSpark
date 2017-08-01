package chapter6

import com.google.gson.Gson
import org.apache.spark.{SparkContext, SparkConf, Accumulator}
import org.apache.spark.rdd.RDD


/**
  * Created by Administrator on 2017/8/1.
  */
object AccumulatorDemo1 {
  val gson = new Gson()

  //  def readJson2Object(input: RDD[String])(classFullName: String)(implicit clazz: Class[_] = Class.forName(classFullName)) = {
  def readJson2Object(input: RDD[String], errorLines: Accumulator[Int])(clazz: Class[_]) = {
    input.flatMap(record => {
      try {
        Some(gson.fromJson(record, clazz))
      } catch {
        case e: Exception => {
          println("读JSON数据失败：" + record)
          errorLines += 1
          None
        }
      }
    })
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("JSONDemo")
    val sc = new SparkContext(conf)
    val input = sc.textFile("file:///E:/first/second/pandainfo.json")

    //创建累加器Accumulator[Int]并初始化为0
    val errorLines = sc.accumulator(0)
    val result = readJson2Object(input, errorLines)(classOf[Person])
    result.map(p => {
      if (p == null) {
        println("空行：" + p)
        errorLines += 1
      } else {
        try {
          val person = p.asInstanceOf[Person]
          if (person.name == null) {
            println("name不能为null：" + person)
            errorLines += 1
          }
          gson.toJson(person)
        } catch {
          case e: Exception => {
            errorLines += 1
            println("JSON转字符串失败：" + p)
          }
        }
      }
    }).saveAsTextFile("file:///E:/first/second/lovesPandas")
    println("共有 " + errorLines + " 行无效数据")
  }
}

case class Person(name: String, lovesPandas: Boolean) {
  override def toString: String = "name: " + name + "\tlovesPandas: " + lovesPandas
}