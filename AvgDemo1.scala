package chapter6

/**
  * Created by Administrator on 2017/8/4.
  */
object AvgDemo1 {
  def combineCtrs(t1: (Int, Int), t2: (Int, Int)) = {
    (t1._1 + t2._1, t1._2 + t2._2)
  }
  def basicAvg(nums: List[Int]) = {
    // 计算平均值
    nums.map((_, 1)).reduce(combineCtrs)
  }
  def main(args: Array[String]) {
    val nums = List(2,3,5,7)
    val result = basicAvg(nums)
    println(result._1 / result._2.toFloat)
  }
}
