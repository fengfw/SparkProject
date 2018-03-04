package com.fengfw.vcvrbid

import java.io.File
import java.text.SimpleDateFormat

import scala.sys.process._
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

class AppendVcvrTask{
}

object AppendVcvrTask {
  def main(args: Array[String]): Unit = {
    val ptime=args(0)
    val hours=Integer.valueOf(args(1))
    val conf = new SparkConf().setAppName(s"AppendVcvrBid_$ptime")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[LongWritable]))
    val sc = new SparkContext(conf)

    val pday=ptime.subSequence(0,8).toString
    val phour=ptime.substring(8,10)
    val rootPath:File=new File("/user/root/flume/express")
    val subFile:File=new File(rootPath.getAbsolutePath+File.separator+"pday="+pday
      +File.separator+"phour="+phour)

    var vcvrPath=s"/user/root/flume/express/pday=$pday/phour=$phour/vcvr*.lzo"
    val vcvrRDD=sc.newAPIHadoopFile[LongWritable, Text, TextInputFormat](vcvrPath)
        .repartition(100)
        .map(line=> {
      val value = line._2.toString
      val sign: String = value.split("\1", -1)(1)
      val bidUuid: String = sign.split("\2", -1)(13)
      (bidUuid, value)
    })

    val impBidRDD=getImpBidRDD(sc,rootPath.getAbsolutePath,pday,phour,hours)
    val outputPath=s"/data/append_vcvr/pday=$pday/phour=$phour"
    val cleanOutputPath = "hdfs dfs -rmr " + outputPath+ "" !
	//将RDD进行join操作，并按条件匹配合并RDD
    val unionRDD: Unit =vcvrRDD.join(impBidRDD).map(a =>{
      val vcvr=a._2._1
      val bid=a._2._2.split("\3")(0)
      val vcvrBid=vcvr+"\3"+bid
      vcvrBid
    })
    .coalesce(20)
//      .repartition(20)
      .saveAsTextFile(outputPath)
    sc.stop()
  }

  //对多个相同的<key,value>形式的RDD进行合并
  def getImpBidRDD(sc:SparkContext,rootPath:String,pday:String,phour:String,hours:Int): RDD[(String, String)] ={
    val hour:Int=Integer.valueOf(phour)
    val dateFormat:SimpleDateFormat=new SimpleDateFormat("yyyyMMddHH")
    var time=dateFormat.parse(pday+phour)
    var impBidPath=rootPath+s"/pday=$pday/phour=$phour/impression_bid*.lzo"
    val pathList:ArrayBuffer[String]=ArrayBuffer(impBidPath)
    for(i <- 0 to hours){
      val oneHourAgo=dateFormat.format(time.getTime-(60*60*1000))
      val newPday=oneHourAgo.subSequence(0,8)
      val newPhour=oneHourAgo.subSequence(8,10)
      time=dateFormat.parse(oneHourAgo)
      pathList+=s"/user/root/flume/express/pday=$newPday/phour=$newPhour/impression_bid*.lzo"
    }
    var impBidRDD=getFileRDD(sc,impBidPath)
    pathList.remove(0)
    for(path <- pathList){
      impBidRDD.union(getFileRDD(sc,path))
    }
    impBidRDD
  }

  //对日志文件按行切片，生成<key,value>形式的RDD
  def getFileRDD(sc:SparkContext,rootPath:String): RDD[(String, String)] ={
    val fileRDD=sc.newAPIHadoopFile[LongWritable, Text, TextInputFormat](rootPath)
      .repartition(100)
      .map(line=>{
      val value=line._2.toString
      val impression:String=value.split("\3",-1)(1)
      val sign:String=impression.split("\1",-1)(1)
      val uuid:String=sign.split("\2",-1)(0)
      (uuid,value)
    })
    fileRDD
  }

}
