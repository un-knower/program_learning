package demo.spark.streaming.demo3

import java.net.ServerSocket
import java.io.PrintWriter

//run 9999 1000
object WordGenerator {
  
  def generateWord(idx:Int):String = {
     import scala.collection.mutable.ListBuffer
     val charList = ListBuffer[Char]()
     (65 to 98).foreach(e => charList += e.toChar)
     val charArray = charList.toArray
     charArray(idx).toString
  }
  
  def randomIdx = {
     import java.util.Random
     val rdx = new Random
     rdx.nextInt(26)
  }

  def main(args: Array[String]): Unit = {
     if(args.length != 2){
        System.err.println("usage:WordGenerator <port> <ms>")
        System.exit(-1)
     }
     val listener = new ServerSocket(args(0).toInt)
     while(true){
        val socket = listener.accept()
        new Thread(){
           override def run = {
              println("got client from "+socket.getInetAddress)
              val out = new PrintWriter(socket.getOutputStream(),true)
              while(true){
                 Thread.sleep(args(1).toLong)
                 val content = generateWord(randomIdx)
                 println(content)
                 out.write(content+"\n")
                 out.flush()
              }
              socket.close()
           }
        }.start()
     }
  }

}