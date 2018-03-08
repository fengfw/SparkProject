package com.fengfw.vcvrbid

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
    val rootPath="/user/flume/express"
    val notMatch="/data/report/vcvr_notmatch"

    val vcvrPath = s"/user/flume/express/pday=$pday/phour=$phour/vcvr*.lzo"
    val vcvrRDD=sc.newAPIHadoopFile[LongWritable, Text, TextInputFormat](vcvrPath)
        .repartition(100)
        .map(line=> {
      val value = line._2.toString
      val sign: String = value.split("\1", -1)(1)
      val bidUuid: String = sign.split("\2", -1)(13)
      (bidUuid, value)
    })

    val impBidRDD=getImpBidRDD(sc,rootPath,pday,phour,hours)
    val outputPath=s"/data/report/append_vcvr/pday=$pday/phour=$phour"
    val cleanOutputPath = "hdfs dfs -rmr " + outputPath+ "" !

    vcvrRDD.join(impBidRDD).map(a =>{
      val vcvrBid=a._2._1+"\3"+a._2._2
      vcvrBid
    })
    .coalesce(20)
//      .repartition(20)
      .saveAsTextFile(outputPath)
    sc.stop()
  }

  def getImpBidRDD(sc:SparkContext,rootPath:String,pday:String,phour:String,hours:Int): RDD[(String, String)] ={
    val dateFormat:SimpleDateFormat=new SimpleDateFormat("yyyyMMddHH")
    var time=dateFormat.parse(pday+phour)
    val impBidPath = rootPath + s"/pday=$pday/phour=$phour/impression_bid*.lzo"
    val pathList:ArrayBuffer[String]=ArrayBuffer(impBidPath)
    for(i <- 0 to hours){
      val oneHourAgo=dateFormat.format(time.getTime-(60*60*1000))
      val newPday=oneHourAgo.subSequence(0,8)
      val newPhour=oneHourAgo.subSequence(8,10)
      time=dateFormat.parse(oneHourAgo)
      pathList+=s"/user/flume/express/pday=$newPday/phour=$newPhour/impression_bid*.lzo"
    }
    val impBidRDD = getFileRDD(sc, pathList(0))
    pathList.remove(0)
    for(path <- pathList){
      impBidRDD.union(getFileRDD(sc,path))
    }
    impBidRDD
  }

  def getFileRDD(sc:SparkContext,rootPath:String): RDD[(String, String)] ={
    val fileRDD=sc.newAPIHadoopFile[LongWritable, Text, TextInputFormat](rootPath)
//      .repartition(20) //减少重分区，可提升计算性能
      .mapPartitions(iterator => {
        val tmpIterator = new scala.collection.mutable.ListBuffer[(String, String)]()
        while (iterator.hasNext) {
          val value=iterator.next().toString
          val bid:String=value.split("\3",-1)(1)
          val adslots=bid.split("\1",-1)(7).split("\2",-1)
          if(adslots.length>1){ //空字段过多，可能导致切割字段数量未达到要求，需判断下所需字段是否在字段组中
            val adslot_type=adslots(1)
            if(adslot_type.equals("video")){ //增加过滤条件，减轻RDD数据集
              val sign:String=bid.split("\1",-1)(1)
              val uuid=sign.split("\2",-1)(0)
              tmpIterator.append((uuid, bid))
            }
          }
        }
        tmpIterator.iterator
      })
    fileRDD
  }
}
