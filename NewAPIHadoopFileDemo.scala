package chapter5

import com.google.gson.Gson
import org.apache.hadoop.io.{LongWritable, IntWritable, Text}
import org.apache.hadoop.mapred.{KeyValueTextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Administrator on 2017/7/21.
  */
object NewAPIHadoopFileDemo {

  val gson = new Gson()
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("NewAPIHadoopFileDemo")
    val sc = new SparkContext(conf)
//    sc.sequenceFile("file:///E:/first/seenPandaNums", classOf[Text], classOf[IntWritable])

//    val output = sc.parallelize(List(("yuanyishan", "97"), ("liushishi", "97")))
//    output.saveAsHadoopFile("file:///E:/first/faceValue", classOf[Text], classOf[Text], classOf[TextOutputFormat[Text, Text]])

    val output = sc.parallelize(List(Girl("zhaoliying", 98), Girl("liuyifei", 80)))
    val jsonOutput = output.map(gson.toJson(_)).map((System.currentTimeMillis(), _))
    jsonOutput.saveAsNewAPIHadoopFile("file:///E:/first/girl2", classOf[LongWritable], classOf[Text],
      classOf[TextOutputFormat[LongWritable, Text]])

    //此方法失败，未知原因
//    jsonOutput.saveAsNewAPIHadoopFile[TextOutputFormat[Long, String]]("file:///E:/first/girl")

  }
}

case class Girl(name: String, faceValue: Int) {
  override def toString: String = name + "\t" + faceValue
}