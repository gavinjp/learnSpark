package chapter6

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD

/**
  * Created by Administrator on 2017/8/4.
  */
object AvgDemo2 {
  def combineCtrs(t1: (Int, Int), t2: (Int, Int)) = {
    (t1._1 + t2._1, t1._2 + t2._2)
  }
  def partitionCtr(partitionNums: Iterator[Int]) = {
    // 计算分区的sumCounter
    var sumNum = 0
    var sumCount = 0
    partitionNums.foreach(num => {
      sumNum += num
      sumCount += 1
    })
    Iterator((sumNum, sumCount))
  }
  def fastAvg(nums: RDD[Int]) = {
    //计算平均值
    val sumCount = nums.mapPartitions(partitionCtr).reduce(combineCtrs)
    sumCount._1 / sumCount._2.toFloat
  }
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]")
      .setAppName("AvgWithMappartitions")
    val sc = new SparkContext(conf)
    val lst = List(2,3,5,7)
    val nums = sc.parallelize(lst, 2)
    println(fastAvg(nums))
  }
}
