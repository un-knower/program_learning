package demo.spark.sql

/**
 * Created by zrh on 16/3/7.
 */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext


case class ZhenshenList(zhenshen: String)

object getPidByBindid {
  def main(args: Array[String]): Unit = {
    val endDateSet = args(0)
    val WXPath = "/user/anti/zengruhong/WX_zhenshen.txt"
    val ZMPath = "/user/anti/zengruhong/ZM_zhenshen.txt"
    val QQPath = "/user/anti/zengruhong/QQ_zhenshen.txt"

    val conf = new SparkConf().
      setAppName(s"BadDebt-zengruhong").
      set("spark.storage.memoryFraction","0.6")

    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    val WXStr = sc.textFile(WXPath,1).filter(_.length > 0)
    val WXMriStr = WXStr.map {
      line => line.split("\t")
    }.cache()

    val ZMStr = sc.textFile(ZMPath,1).filter(_.length > 0)
    val ZMMriStr = ZMStr.map {
      line => line.split("\t")
    }.cache()

    val QQStr = sc.textFile(QQPath,1).filter(_.length > 0)
    val QQMriStr = QQStr.map {
      line => line.split("\t")
    }.cache()

    val WXZhenshenList = WXMriStr.
      filter(x=>x.length>0).
      map(
        x => ZhenshenList(x(0))
    ).toDF()

    val ZMZhenshenList = ZMMriStr.
    filter(x=>x.length>0).
      map(
        x => {
          ZhenshenList(x(0))
        }
      ).toDF()

    val QQZhenshenList = QQMriStr.
      filter(x=>x.length>0).
      map(
        x => {
          ZhenshenList(x(0))
        }
      ).toDF()

    val pid_null = sc.parallelize(Array(" "))
    if(WXZhenshenList.count()>0){
      WXZhenshenList.registerTempTable("WXZhenshenList")
      val pid_wx = hiveContext.sql(s"""
      SELECT b_wx_bindid, string(concat_ws(',', collect_set(string(uid)))) as uid_set
      FROM pdw.p_identity
      JOIN WXZhenshenList
      ON p_identity.b_wx_bindid = WXZhenshenList.zhenshen
      WHERE concat (year,month,day) = '${endDateSet}'
      GROUP BY b_wx_bindid
      """).cache
      pid_wx.write.format("com.databricks.spark.csv")
        .option("header", "false")
        .save(s"zengruhong/wx_pid_bindid.csv")
    }
    else {
      pid_null.saveAsTextFile("zengruhong/wx_pid_bindid.csv")
    }
    if(ZMZhenshenList.count()>0){
      ZMZhenshenList.registerTempTable("ZMZhenshenList")
      val pid_zm = hiveContext.sql(s"""
      SELECT b_zmxy_bindid, concat_ws(',', collect_set(string(uid))) as uid_set
      FROM pdw.p_identity
      JOIN ZMZhenshenList
      ON p_identity.b_zmxy_bindid = ZMZhenshenList.zhenshen
      WHERE concat (year,month,day) = '${endDateSet}'
      GROUP BY b_zmxy_bindid
      """).cache
      pid_zm.write.format("com.databricks.spark.csv")
        .option("header", "false")
        .save(s"zengruhong/zm_pid_bindid.csv")
    }
    else {
      pid_null.saveAsTextFile("zengruhong/zm_pid_bindid.csv")
    }
    if(QQZhenshenList.count()>0){
      QQZhenshenList.registerTempTable("QQZhenshenList")
      val pid_qq = hiveContext.sql(s"""
      SELECT b_qq_bindid, concat_ws(',', collect_set(string(uid))) as uid_set
      FROM pdw.p_identity
      JOIN QQZhenshenList
      ON p_identity.b_qq_bindid = QQZhenshenList.zhenshen
      WHERE concat (year,month,day) = '${endDateSet}'
      GROUP BY b_qq_bindid
      """).cache
      pid_qq.write.format("com.databricks.spark.csv")
        .option("header", "false")
        .save(s"zengruhong/qq_pid_bindid.csv")
    }
    else {
      pid_null.saveAsTextFile("zengruhong/qq_pid_bindid.csv")
    }
  }

}
