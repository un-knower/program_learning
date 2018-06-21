package demo.spark.batch.broadcast

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

object IpUtil {
  /*
     * 将ip地址转换成数字long
     */
    def ip2Long(ip:String):Long = {
        val fragments = ip.split("[.]") 
        var ipNum = 0L
        for(i <- 0 until fragments.length) {
            ipNum = fragments(i).toLong | ipNum << 8L
        }
        ipNum
    }
    
    def ip2Long2(ip:String) = {
        val fragments = ip.split("[.]")
        val ipNum = 16777216L*fragments(0).toLong+65536L*fragments(1).toLong+
                256L*fragments(2).toLong + fragments(3).toLong
        ipNum
    }
    /**
     * 将数字转换成ip地址
     */
    def long2Ip(ipNum:Long) = {
        val mask = List(0x000000FF,0x0000FF00,0x00FF0000,0xFF000000)
        val ipInfo = new StringBuffer
        var num = 0L
        for(i <- 0 until 4) {
            num = (ipNum & mask(i)) >> (i*8)
            if(i>0) ipInfo.insert(0, ".")
            ipInfo.insert(0, num)
        }
        ipInfo
    }
    def readIpFile(file:String) = {
        val data = scala.io.Source.fromFile(file).mkString
        data
    }
    /*hashmap法*/
    def ipContentToMap(ipContent:String) = {
        val map = new HashMap[Long,String]()
        val lines = ipContent.split("\n")
        val minBound = lines(0).split(",")(2).toLong
        val maxBound = lines(lines.length-1).split(",")(3).toLong
        for(line <- lines) {
            val fields = line.split(",")
            map += (fields(2).toLong -> "ERRPR,ERROR,ERROR")
            map += (fields(3).toLong -> line)
        }
        (map,minBound,maxBound)
    }
    def getContentByKey(map: HashMap[Long, String], minBound:Long ,maxBound:Long, key:Long) = {
        val notFound = "NOTFOUND"
        if(key > maxBound || key < minBound) notFound
        else {
            var keyTemp = key
            var result:String = map.getOrElse(keyTemp, notFound)
            while(notFound.eq(result)) {
                keyTemp += 1L
                result = map.getOrElse(keyTemp, notFound)
            }
            result
        }
        
    }
    /*二分查找法*/
    def ipContentToArryBuffer(ipContent:String) = {
        val arrayBuff = new ArrayBuffer[(String,String,String)]()
        val lines = ipContent.split("\n")
        for(line <- lines) {
            val fields = line.split(",")
            val res = ( fields(2) , fields(3), fields(4))
            arrayBuff +=  res
        }
        arrayBuff
    }
    def binarySearch(lines:Array[(String,String,String)], ipNum:Long):Int ={
        var low = 0
        var high = lines.length -1 
        while(low <= high) {
            val middle = (low+high)/2
            val midLeft = lines(middle)._1.toLong
            val midRight = lines(middle)._2.toLong
            if((ipNum >= midLeft) && ipNum <= midRight) {
                return middle
            }
            if(ipNum < midLeft) {
                high = middle -1
            }else {
                low = middle +1
            }
        }
        return -1
    }
    
    def searchContentByKey(lines:Array[(String,String,String)], ipNum:Long):String= {
        val idx = binarySearch(lines, ipNum)
        if(idx == -1) {
            "ERROR,ERROR,ERROR"
        }else {
            lines(idx)._1+","+lines(idx)._2+","+lines(idx)._3
        }
    }
    def main(args: Array[String]): Unit = {
        val ip = "102.160.85.168"
        println(2 << 8L)
        val ipLong = ip2Long(ip)
        println(ipLong)
        println(ip2Long2(ip))
        println(long2Ip(ipLong))
        
        
        val file = "src\\com\\spark\\demo\\broadcast\\ip-by-country.csv"
        val ipContent = readIpFile(file)
        /**使用hashmap方法
         * */
        val (ip2ContentMap, minBound, maxBound) = ipContentToMap(ipContent)
        val key = 1721783720L
        println(getContentByKey(ip2ContentMap, minBound, maxBound,key))
        
        /*使用二分查找法
         * 因为ipNum数据是排好序的
         * */
        val lines = ipContentToArryBuffer(ipContent)
        println(searchContentByKey(lines.toArray, key))
        
        
    }
}