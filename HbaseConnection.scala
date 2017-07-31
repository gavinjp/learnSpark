package chapter5

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkContext, SparkConf}


/**
  * Created by Administrator on 2017/7/31.
  */
object HbaseConnection {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("HBaseConnection")
    val sc = new SparkContext(conf)
    val hconf = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", "mini5,mini6,mini7")
    hconf.set("hbase.zookeeper.property.clientPort", "2181")
    hconf.set(TableInputFormat.INPUT_TABLE, "sparkHbase")
    val hbaseRdd = sc.newAPIHadoopRDD(hconf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    //遍历输出
    hbaseRdd.foreach{ case (_,result) =>
      val key = Bytes.toString(result.getRow)
      val name = Bytes.toString(result.getValue("user".getBytes,"name".getBytes))
      val age = Bytes.toString(result.getValue("user".getBytes,"age".getBytes))
      println("rowkey:"+key+" name:"+name+" age:"+age)
    }
  }
}
