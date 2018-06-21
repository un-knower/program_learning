package demo.spark.sql

/**
 * Created by zrh on 16/3/4.
 */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext


case class passengerList(p_phone: String)

object getOrderNumByPhone {
  def main(args: Array[String]): Unit = {

    val fromDate = args(0)
    val endDate = args(1)
    val delayDate = args(2)
    //val p_phone = "13121927297"
    val filePath = "/user/anti/zengruhong/passenger_phone.txt"

    println("fromDate" + fromDate)
    println("endDate" + endDate)
    println("delayDate" + delayDate)

    /* query */

    val conf = new SparkConf().
      setAppName(s"BadDebt-zengruhong").
      set("spark.storage.memoryFraction","0.6")

    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    val examplesStr = sc.textFile(filePath,1).filter(_.length > 0)
    println("--- original first row is  " + examplesStr.first().toString + "   ---")
    val sampleMriStr = examplesStr.map {
      line => line.split(",")
    }.cache()
    sampleMriStr.first()


    val passenger_phone = sampleMriStr.map(x => passengerList(x(0))).toDF()

    passenger_phone.registerTempTable("passenger_phone_table")


    val order_lastTime = hiveContext.sql(s"""
    SELECT order_id, first(passenger_phone) AS passenger_phone_first, max(a_modify_time) AS last_time
    FROM gulfstream_ods.g_order
    JOIN passenger_phone_table
    ON p_phone = passenger_phone
    WHERE concat (year,month,day) >= '${fromDate}'
    and concat (year,month,day) <= '${delayDate}'
    GROUP BY order_id
    """).cache
    order_lastTime.registerTempTable("order_lastTime")

    val order_status = hiveContext.sql(s"""
    SELECT order_lastTime.order_id, g_order.passenger_phone, g_order.order_status
    FROM gulfstream_ods.g_order
    JOIN order_lastTime
    ON order_lastTime.order_id = g_order.order_id
    AND order_lastTime.last_time = g_order.a_modify_time
    WHERE concat (year,month,day) >= '${fromDate}'
    AND concat (year,month,day) <= '${delayDate}'
    """).cache
    order_status.registerTempTable("order_status")

    val valid_order_status = hiveContext.sql(s"""
    SELECT order_status.order_id, order_status.passenger_phone,
    order_status.order_status
    FROM order_status
    JOIN gulfstream_ods.g_order
    ON order_status.order_id = g_order.order_id
    WHERE concat (year,month,day) >= '${fromDate}'
    AND concat (year,month,day) <= '${endDate}'
    """).cache
    valid_order_status.registerTempTable("valid_order_status")

    val p_order_num_sum =  hiveContext.sql(s"""
    SELECT passenger_phone, count(order_id) as count_sum
    FROM valid_order_status
    GROUP BY passenger_phone
    """).cache
    p_order_num_sum.registerTempTable("p_order_num_sum")

    val p_order_num =  hiveContext.sql(s"""
    SELECT valid_order_status.passenger_phone, first(count_sum), count(order_id)
    FROM valid_order_status
    JOIN p_order_num_sum
    ON p_order_num_sum.passenger_phone = valid_order_status.passenger_phone
    WHERE order_status = 5
    GROUP BY valid_order_status.passenger_phone
    """).cache

    p_order_num.write.format("com.databricks.spark.csv") .option("header", "false") .save(s"zengruhong/p_order_num_${fromDate}_${endDate}_${delayDate}.csv")

  }
}
