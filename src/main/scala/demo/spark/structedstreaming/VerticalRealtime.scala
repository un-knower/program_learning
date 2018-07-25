package demo.spark.structedstreaming

import java.sql.Timestamp
import java.util.Date

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.{SparkSession, functions}

//                  2S5Wcx/2x1kfBk63z EV/RCC/DU 科技、娱乐、体育、财经  deviceid      ios/android     时长   是否是点击   一条EV总文章数    时间戳
case class VerticalData(id:String, event:String, column:String, deviceid:String, platform:String, du:Int, isrcc:Int, datalen:Int, timestamp:Timestamp)

object VerticalRealtime {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
                            .master("local[2]")
      .config("spark.sql.shuffle.partitions", 3)
      .config("spark.default.parallelism", 3)
                            .appName("")
                            .getOrCreate()
    import spark.implicits._
    val data = spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "SparkMaster:9092")
          .option("subscribe", "flinktest")
          .load()
          .selectExpr("CAST(value AS STRING)").as[String]


    val verticalData  = data.filter(x => {
      (x.contains("2S5Wcx")|| x.contains("2x1kfBk63z")) &&
      (x.contains("\"n\":\"EV\"") || x.contains("\"n\":\"RCC\"") || x.contains("\"n\":\"DU\"")) &&
      (
        x.contains("\"column\":\"财经\"") || x.contains("\"g\":\"财经\"") ||
        x.contains("\"column\":\"娱乐\"") || x.contains("\"g\":\"娱乐\"") ||
        x.contains("\"column\":\"体育\"") || x.contains("\"g\":\"体育\"") ||
        x.contains("\"column\":\"科技\"") || x.contains("\"g\":\"科技\"")
      )
    }).map(x =>  {
      try {
        val jsonObject = JSON.parseObject(x)
        val timestamp = jsonObject.getTimestamp("timestamp")
        val id = jsonObject.getString("id")
        val deviceid = jsonObject.getString("devId")
        val event = jsonObject.getString("n")

        val platform = jsonObject.getString("platform")
        var column = ""
        var du = 0
        var isrcc = 0
        var datalen = 0
        event match {
          case "EV" => {
            val kvJsonObject = jsonObject.getJSONObject("kv")
            column = kvJsonObject.getString("column")
            val data = kvJsonObject.getString("ids")
            datalen = data.split(",").length
          }
          case "RCC" => {
            val kvJsonObject = jsonObject.getJSONObject("kv")
            column = kvJsonObject.getString("column")
            isrcc = 1
          }
          case "DU" => {
            column = jsonObject.getString("g")
            du = jsonObject.getInteger("du")

          }
          case _ => {
          }
        }
        VerticalData(id, event, column, deviceid, platform, du, isrcc, datalen, timestamp)
      } catch {
        case _ : Exception => {
          VerticalData("", "", "", "", "", 0, 0, 0, new Timestamp(new Date().getTime))
        }
      }

    }).withWatermark("timestamp", "10 seconds")

   // verticalData.createOrReplaceTempView("vertical")

    val ev = verticalData.where("event =  'EV'").groupBy(functions.window(verticalData.col("timestamp"), "10 seconds")).agg(
      functions.approx_count_distinct("deviceid").alias("uv"),
      functions.count("deviceid").alias("pv"),
      functions.sum("datalen").alias("ev_doc_sum")
    )

    val all = verticalData.groupBy(functions.window(verticalData.col("timestamp"), "10 seconds")).agg(
      functions.sum("isrcc").alias("count_rcc"),
      functions.sum("du").alias("du")
    )

    ev.writeStream.outputMode("update").format("console").option("truncate","false").start()
    all.writeStream.outputMode("update").format("console").option("truncate","false").start()

    spark.streams.awaitAnyTermination()
  }
}
