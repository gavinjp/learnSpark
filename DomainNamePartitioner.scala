package chapter4

import java.net.URL

import org.apache.spark.Partitioner

/**
  * Created by Administrator on 2017/7/15.
  */
class DomainNamePartitioner(numParts: Int) extends Partitioner{
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    val domain = new URL(key.toString).getHost
    val code = domain.hashCode % numPartitions
    if (code < 0) {
      code + numPartitions //使其非负
    } else {
      code
    }
  }

  //用来让Spark区分分区函数对象的Java equals方法
  override def equals(other: scala.Any): Boolean = other match {
    case dnp: DomainNamePartitioner =>
      dnp.numPartitions == numPartitions
    case _ =>
      false
  }
}
