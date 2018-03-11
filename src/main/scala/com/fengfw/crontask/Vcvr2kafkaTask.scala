package com.fengfw.vcvrbid

import java.util.Properties

import com.typesafe.config.ConfigFactory
import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
import org.apache.spark.{SparkConf, SparkContext}

class Vcvr2kafkaTask {

}
object Vcvr2kafkaTask {
  def main(args: Array[String]) {
    val config = ConfigFactory.parseFile(new java.io.File("conf/vcvr2kafka.conf"))
    val ptime = args(0)
    val pday = ptime.subSequence(0, 8).toString
    val phour = ptime.substring(8, 10)
    val rootPath = config.getString("spark.monitorPath")
    val topic = config.getString("spark.topic")
    //    val inputdir = "/data/vcvr/pday=20180228/phour=09"
    val inputdir = rootPath + s"pday=$pday/phour=$phour"
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"VcvrToKafka$pday$phour")
    val sc = new SparkContext(sparkConf)
    val brokerAddr = config.getString("spark.bootstrapList")
    val input = sc.textFile(inputdir).coalesce(50)

    input.foreachPartition { partitionOfRecords =>
      val producer = new Producer[String, String](new ProducerConfig(getProducerConfig(brokerAddr)))
      partitionOfRecords.foreach { message =>
        producer.send(new KeyedMessage[String, String](topic, message))
      }
      producer.close
    }
	
    sc.stop()
  }

  def getProducerConfig(brokerAddr: String): Properties = {
    val props = new Properties()
    props.put("metadata.broker.list", brokerAddr)
    props.put("request.required.acks", "1")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props
  }
}
