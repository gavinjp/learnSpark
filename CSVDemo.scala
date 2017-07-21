package chapter5

import java.io.{StringWriter, StringReader}
import java.util
import com.opencsv.{CSVWriter, CSVReader}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2017/7/19.
  */
object CSVDemo {
  def readNormalCSV(input: RDD[String]): Unit = {
    val result = input.map(line => {
      val reader = new CSVReader(new StringReader(line))
      reader.readNext()
    })
    result.foreach(ss => {
      ss.foreach(s => {
        print(s + " ")
      })
      println()
    })
  }

  def readUnNormalCSV(input: RDD[(String, String)]): RDD[ListBuffer[Person2]] = {
    val result = input.map(t => {
      val txt = t._2
      val reader = new CSVReader(new StringReader(txt))
      val lst = reader.readAll()

      //对不正常数据进行处理
      val personList = ListBuffer[Person2]()
      var normalIndex = 0 // 上一个正常数据的索引
      var count = 0 //统计不正常的行数
      for (i <- 0 to lst.size() - 1) {
        val s = lst.get(i)

        // 获取正常的字符数组的大小（初始假定第一行的字符数组大小是正常值）
        // （如果某字段有换行符，则下一行的size一定比本行的小）
        var normalSize = lst.get(normalIndex).size

        //正常值
        if (s.size >= normalSize) {
          normalIndex = i
          normalSize = s.size
          personList += Person2(s(0).trim, s(1).trim)
        }

        //说明这是不正常的数据，是上一行某字段换行后的数据
        if (s.size < normalSize) {
          count += 1
          val s_normal = lst.get(normalIndex)
          s_normal(1) = s_normal(1).trim + "\t" + s(0).trim
          personList(i - count) = Person2(s_normal(0).trim, s_normal(1).trim)
        }
      }
      personList
    })
    result
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("CSVDemo")
    val sc = new SparkContext(conf)
    /*val input = sc.textFile("file:///E:/first/second/favourite_animals.csv")
    readNormalCSV(input)*/

    val input = sc.wholeTextFiles("file:///E:/first/second/favourite_animals")
    val result = readUnNormalCSV(input)

    // 聚合
    /*val tuple = result.flatMap(_.map(x => (x.name, x.favoriteAnimal))).reduceByKey((x, y) => x + "\t" + y)
    tuple.saveAsTextFile("file:///E:/first/second/favourite_animal_result2")*/



    // 聚合，去重
    /*val tuple = result.flatMap(_.map(x => (x.name, x.favoriteAnimal))).reduceByKey((x, y) => {
      var result = x
      y.split("\t").foreach(tempY => {
        if (!x.split("\t").contains(tempY)) {
          result += "\t" + tempY
        }
      })
      result
    })
    tuple.saveAsTextFile("file:///E:/first/second/favourite_animal_result")*/

    result.flatMap(_.map(x => List(x.name, x.favoriteAnimal).toArray)).mapPartitions(people => {
      val lst = new util.ArrayList[Array[String]]()
      people.foreach(x => {
        lst.add(x)
      })
      val stringWriter = new StringWriter()
      val writer = new CSVWriter(stringWriter)
      writer.writeAll(lst)
      Iterator(stringWriter.toString)
    }).saveAsTextFile("file:///E:/first/second/favourite_animal_result3")

  }
}

case class Person2(name: String, favoriteAnimal: String) {
  override def toString: String = name + "\t" + favoriteAnimal
}