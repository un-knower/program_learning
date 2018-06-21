package demo.spark.streaming.demo2

import scala.util.Random
import java.net.ServerSocket
import java.io.PrintWriter

/**
 * Created with IntelliJ IDEA.
 * User: qingjian
 * Date: 16-5-25
 * Time: 下午4:44
 * //随机生成“产品活动”的消息生成端
 * //每秒最多5个，然后通过网络连接发送
 */
object StreamingProducer {
  def main(args:Array[String]):Unit ={
    val random = new Random()

    //每秒最大活动数
    val MaxEvents = 6

    //读取可能的名称
    val namesResource = this.getClass.getResourceAsStream("names.csv") //换其他路径不行
    println(namesResource)
    val names = scala.io.Source.fromInputStream(namesResource)
                .getLines()
                .toList
                .mkString(",")
                .split(",")
                .toSeq
                
              
                    
    //println(names)
    /**
     * 生成一系列可能的产品
     */
    def products = Seq(
        "iPhone Cover" -> 9.99,
        "Headphones" -> 5.49,
        "Samsung Galaxy Cover" -> 8.95,
        "iPad Cover" -> 7.49
    )
    
    /**
     * 生成随机产品活动
     */
    def generateProductEvents(n:Int) = {
        ( 1 to n).map{i=>
            val (product, price)=
                products(random.nextInt(products.size))
            val user = random.shuffle(names).head
            (user, product, price)
        }
        
    }
    
    
    
    //创建网络生成器
    val listener = new ServerSocket(9999)
    println("Listening on port: 9999")
    
    while(true) {
        val socket = listener.accept()
        new Thread() {
            override def run={
                println("Got client connected "+socket.getInetAddress)
                val out = new PrintWriter(socket.getOutputStream,true)
                while(true){
                    Thread.sleep(1000) //1s
                    val num = random.nextInt(MaxEvents)
                    val productEvents = generateProductEvents(num)
                    productEvents.foreach{event=>
                        out.write(event.productIterator.mkString(","))  
                        println(event.productIterator.mkString(","))  
                        out.write("\n")
                    }
                    out.flush
                    println(s"Created $num events..")
                }
                socket.close()
                
                
            }
             
            
        }.start()
        
        
        
    }
    

  }

}