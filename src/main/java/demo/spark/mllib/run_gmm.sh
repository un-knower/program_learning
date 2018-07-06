#!/bin/bash

#usage:
#sh run_gmm.sh hive_tablename sf
#sh run_gmm.sh hive_tablename taxi
#sh run_gmm.sh hive_tablename kc
#sh run_gmm.sh hive_tablename sf 20160919


filepath=$(cd "$(dirname "$0")"; pwd)
jarpath=$filepath/../../../../bazel-bin/java/com/didi/anomaly/GmmEM.jar
resultpath=/user/wguangliang/output
retryNum=10
sec=7
today=`date "+%Y%m%d"`
yesterday=`date -d "1 days ago" +%Y%m%d`
time=$yesterday
hive_tablename=$1     #antispam.antispam__zc_order_merger_result,antispam.antispam__feature_center_sf,antispam.antispam__feature_center_taxi
productid=$2
if [[ $productid != "sf" && $productid != "kc" && $productid != "taxi" ]];then
 echo "usage"
 echo "sh run_gmm.sh hive_tablename sf"
 echo "sh run_gmm.sh hive_tablename taxi"
 echo "sh run_gmm.sh hive_tablename kc"
 echo "sh run_gmm.sh hive_tablename sf 20160919"
 exit -1
fi
if [ $# -eq 3 ];then
  time=$3
fi


export SPARK_HOME=/data/hadoop/spark
export PATH=$SPARK_HOME/bin:$PATH
export HADOOP_HOME=/data/hadoop/hadoop-2.7.2

$HADOOP_HOME/bin/hadoop fs -rmr $resultpath/$productid/result/$time
$HADOOP_HOME/bin/hadoop fs -rmr $resultpath/$productid/abnormal/$time



for((i=1;i<=${retryNum};i++)) #retry
do

  #spark submit
  echo "$today task begin"
  $SPARK_HOME/bin/spark-submit \
  --num-executors 15 \
  --class com.didi.anomaly.GmmEM \
  $jarpath \
  -i $hive_tablename  -u oid -v first_order_flag,first_order_ratio,non_local_flag,non_local_ratio,passenger_group_size,group_order_count,trade_index,spamflag -w spamflag -e test.antispam_cblof -f $productid -d $time -o $resultpath/$productid/result/$time -p $resultpath/$productid/abnormal/$time


  ret=$?
  if [ $ret -ne 0 ]
  then
    echo "err:spark-submit return="$ret";retry num=$i, sleep $sec seconds..."
    sleep $sec
  else
    echo "$today task done!"
    break
  fi
done



