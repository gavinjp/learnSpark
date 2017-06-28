package chapter2

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2017/6/28.
  */
object WordCountDemo {
  def main(args: Array[String]) {
    // 创建一个Scala版本的Spark Context
    val conf = new SparkConf().setMaster("local").setAppName("gavin's word count demo")
    val sc = new SparkContext(conf)
    // 读取输入数据
    val input = sc.textFile(args(0))
    // 把它切分成一个个单词
    val words = input.flatMap(line => line.split(" "))
    // 转换为键值对并计数
    val counts = words.map(word => (word, 1)).reduceByKey((a,b) => a+b)
    // 将统计出来的单词总数存入一个文本文件，引发求值
    counts.saveAsTextFile(args(1))
  }
}
