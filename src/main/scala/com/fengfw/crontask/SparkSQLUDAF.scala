package com.fengfw.crontask

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import scala.sys.process._

/**
  * Spark SQL UDAS：user defined aggregation function
  * UDF: 函数的输入是一条具体的数据记录，实现上讲就是普通的scala函数-只不过需要注册
  * UDAF：用户自定义的聚合函数，函数本身作用于数据集合，能够在具体操作的基础上进行自定义操作
  */
class SparkSQLUDAF{

}
object SparkSQLUDAF {
  def main(args: Array[String]): Unit = {
    val pday=args(0)
    val conf = new SparkConf().setAppName(s"sparksqlUDAF_$pday")

    val sc = new SparkContext(conf)

    val hiveSqlCtx = new HiveContext(sc)

    val aggregateSql="select partner_id,sum(total_cost) as total_cost,sum(unbid) as unbid," +
        "sum(bid) as bid,sum(imp) as imp,sum(click) as click,sum(reach) as reach,pday,phour,pyid," +
        "toextract(pytags) as pytags,client_id,campaign_division_id,sub_campaign_division_id," +
      "exe_campaign_division_id,sub_platform from rpt_effect_base " +
      s"where pday=$pday group by partner_id,pday,phour,pyid,client_id,campaign_division_id," +
        "sub_campaign_division_id,exe_campaign_division_id,sub_platform"
    hiveSqlCtx.udf.register("toextract",new ToExtract)
//    hiveSqlCtx.sql("set hive.exec.dynamic.partition=true;")
//    hiveSqlCtx.sql("set hive.exec.dynamic.partition.mode=nonstrict")
//    hiveSqlCtx.sql(aggregateSql)
    val outputPath = s"hdfs://hadoopcluster/tmp/rpt_crontask_source/pday=$pday"
    val cleanOutputPath = "hadoop fs -rmr " + outputPath+ "" !

    hiveSqlCtx.sql(aggregateSql).write.format("orc").partitionBy("advertiser_id").save(outputPath)

  }

}

