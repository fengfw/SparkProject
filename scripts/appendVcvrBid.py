#coding:utf-8

import commands
import time
import datetime
import sys

reload(sys)
sys.setdefaultencoding("utf8")

def getDate(hours=2):
     nowTime = datetime.datetime.now()
     delayTime = nowTime - datetime.timedelta(hours=hours)
     delayTime = delayTime.strftime("%Y%m%d%H")
     return delayTime

if __name__ == "__main__":
    from optparse import OptionParser
    usage = 'usage: appendVCVR.py -d date'
    parser = OptionParser(usage=usage, version="%crontask_base_load 1.0")
    parser.add_option('-b', '--beginTime', dest='beginTime',default=getDate(2), help="beginTime")
    parser.add_option('-s', '--hours', dest='hours', help="hours")

    (options, args) = parser.parse_args()

    target_time = getDate(2)
    hours ='2'
    print("target_time=%s" % (target_time))
    submit_command='''spark-submit --class com.fengfw.vcvrbid.AppendVcvrTask --master yarn-client --conf spark.io.compression.codec=org.apache.spark.io.LZ4CompressionCodec --conf spark.memory.useLegacyMode=true --conf spark.shuffle.memoryFraction=0.8 --conf spark.storage.memoryFraction=0.1 --conf spark.storage.unrollFraction=0.1 --conf spark.dynamicAllocation.enabled=false --conf spark.network.timeout=1200s --executor-memory 8G --num-executors 80 --conf spark.sql.shuffle.partitions=160 --name SparkRepartitoinBase_source%s --queue normal /home/storm/fuwei.feng/vcvr-bid/target/vcvrbid-1.0-SNAPSHOT.jar %s %s'''% (target_time,target_time,hours)
    print "run command = %s" % submit_command
    ret = commands.getoutput(submit_command)
    print "run command = %s" % ret
