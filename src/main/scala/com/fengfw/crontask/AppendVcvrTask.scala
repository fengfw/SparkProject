package com.fengfw.vcvrbid

import java.text.SimpleDateFormat

import scala.sys.process._
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.broadcast.Broadcast
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

    val pday=ptime.substring(0,8)
    val phour=ptime.substring(8,10)
    val bidRootPath="/user/flume/express"

    val vcvrPath = s"/user/root/flume/express/pday=$pday/phour=$phour/vcvr*.lzo"
    val vcvrRDD=sc.newAPIHadoopFile[LongWritable, Text, TextInputFormat](vcvrPath)
        .repartition(100)
        .map(line=> {
      val value = line._2.toString
      val sign: String = value.split("\1", -1)(1)
      val bidUuid: String = sign.split("\2", -1)(13)
      (bidUuid, value)
    }).persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER)

    val vcvrID=vcvrRDD.map(a=>(a._1,"")).collect()
    val broadcast=sc.broadcast(vcvrID)

    val impBidRDD=getImpBidRDD(sc,bidRootPath,ptime,hours,broadcast)
      .persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER)

    val outputMatchPath=s"/data/production/report/append_vcvr/pday=$pday/phour=$phour"
    val outputNotMatchPath=s"/data/production/report/vcvr_notmatch/pday=$pday/phour=$phour"
    val cleanoutputMatchPath = "hdfs dfs -rmr " + outputMatchPath+ "" !
    val cleanoutputNotMatchPath = "hdfs dfs -rmr " + outputNotMatchPath+ "" !

    val result=vcvrRDD.leftOuterJoin(impBidRDD).persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER)

    result.filter(!_._2._2.getOrElse("").equals("")).map(a =>{
      val vcvrBid=a._2._1+"\3"+a._2._2
      vcvrBid
    }).coalesce(20)
      //      .repartition(20)
      .saveAsTextFile(outputMatchPath)

    result.filter(_._2._2.getOrElse("").equals("")).map(b=>b._2._1).saveAsTextFile(outputNotMatchPath)

    sc.stop()
  }

  //将多个小时时间段的RDD合并
  def getImpBidRDD(sc:SparkContext,rootPath:String,pday:String,phour:String,hours:Int,broadcast:Broadcast[Map[String,String]]): RDD[(String, String)] ={
    val dateFormat:SimpleDateFormat=new SimpleDateFormat("yyyyMMddHH")
    var time=dateFormat.parse(pday+phour)
    val impBidPath = rootPath + s"/pday=$pday/phour=$phour/impression_bid*.lzo"
    val pathList:ArrayBuffer[String]=ArrayBuffer(impBidPath)
    for(i <- 0 to hours){
      val oneHourAgo=dateFormat.format(time.getTime-(60*60*1000))
      val newPday=oneHourAgo.subSequence(0,8)
      val newPhour=oneHourAgo.subSequence(8,10)
      time=dateFormat.parse(oneHourAgo)
      pathList+=s"pday=$newPday/phour=$newPhour"
    }
    val path="/user/flume/express/{"+pathList.mkString(",")+"}/impression_bid*.lzo"
    val impBidRDD = getFileRDD(sc, path,broadcast)
//    pathList.remove(0)
//    for(path <- pathList){
//      impBidPath=impBidRDD.union(getFileRDD(sc,path))
//    }
    impBidRDD
  }

  //将指定目录下文件生成（key，value）形式RDD
  def getFileRDD(sc:SparkContext,rootPath:String,broadcast:Broadcast[Map[String,String]]): RDD[(String, String)] ={
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
              if(broadcast.value.contains(uuid)){ //过滤掉vcvr中未出现的bid的uuid，提升后期匹配性能
                tmpIterator.append((uuid, bid))
              }
            }
          }
        }
        tmpIterator.iterator
      })
    fileRDD
  }
}
