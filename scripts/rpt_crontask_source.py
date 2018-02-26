#coding:utf-8

import commands
import time
import datetime
import hashlib, urllib2, sys
import urllib

reload(sys)
sys.setdefaultencoding("utf8")

def getDate(pastdays=3):
     today = datetime.date.today()
     yesterday = today - datetime.timedelta(days=pastdays)
     yesterday = yesterday.strftime("%Y%m%d")
     return yesterday

if __name__ == "__main__":
    from optparse import OptionParser
    usage = 'usage: rpt_crontask_source.py -d date'
    parser = OptionParser(usage=usage, version="%crontask_base_load 1.0")
    parser.add_option('-s', '--starttime', dest='starttime',default=getDate(3), help="starttime")
    parser.add_option('-e', '--endtime', dest='endtime',default=getDate(2), help="endtime")

    (options, args) = parser.parse_args()

    if options.starttime is None or options.endtime is None or options.endtime<options.starttime:
        print usage
    else:
        s_day = int(time.mktime(time.strptime(options.starttime, '%Y%m%d')))
        e_day = int(time.mktime(time.strptime(options.endtime, '%Y%m%d')))
        target_date = ""
        cmd_list = []

        for i in range(s_day, e_day, 3600*24):
            begin_date = time.strftime('%Y%m%d', time.gmtime(i))
            target_date = begin_date
            print "begin_date=%s, target_date=%s" % (begin_date, target_date)
            submit_command='''spark-submit --class com.ipinyou.crontask.SparkSQLUDAF --master yarn-client --conf spark.io.compression.codec=org.apache.spark.io.LZ4CompressionCodec --conf spark.memory.useLegacyMode=true --conf spark.shuffle.memoryFraction=0.8 --conf spark.storage.memoryFraction=0.1 --conf spark.storage.unrollFraction=0.1 --conf spark.dynamicAllocation.enabled=false --conf spark.network.timeout=1200s --executor-memory 8G --num-executors 120 --conf spark.sql.shuffle.partitions=200 --name SparkRepartitoinBase_source%s --queue normal /data/users/data-subscr/fuwei.feng/pytagstest/target/pytagstest-1.0-SNAPSHOT.jar %s'''% (target_date, target_date)
            print "run command = %s" % submit_command
            ret = commands.getoutput(submit_command)

            print "run command = %s" % ret