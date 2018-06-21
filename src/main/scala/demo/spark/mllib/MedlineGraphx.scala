package demo.spark.mllib

import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.spark.SparkConf
import scala.xml.XML
import scala.xml.Elem
import org.apache.spark.rdd.RDD
import com.google.common.hash.Hashing
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph

object MedlineGraphx {
    def loadMedline(sc:SparkContext, path:String) = {
        @transient val conf = new Configuration()
        conf.set(XmlInputFormat.START_TAG_KEY, "<MedlineCitation ")
        conf.set(XmlInputFormat.END_TAG_KEY, "</MedlineCitation>")
        val in = sc.newAPIHadoopFile(path, classOf[XmlInputFormat],classOf[LongWritable],classOf[Text],conf)
        in.map(line=>{line._2.toString})    
    }
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("").setMaster("local[4]")
        val sc = new SparkContext(conf)
        val medline_raw = loadMedline(sc, "file/medline16n0033.xml")
        medline_raw.take(1).foreach { println }
        println("*"*10)
        val raw_xml = medline_raw.take(1)(0)
        val elem = XML.loadString(raw_xml)
        println(elem.label) //MedlineCitation
        println(elem.attributes) // Status="MEDLINE" Owner="PIP"
        
        (elem \\ "MeshHeadingList").map(_.text).foreach(println)
        
        //每个DescriptorName条目都有一个MajorTopicYN属性，它表示该MeSH标签是否是所引用的文章的主要主题。
        //只要我们在XML标签属性前加上@符号，就可以用\和\\运算符得到XML标签属性的值
        def majorTopics(elem:Elem):Seq[String]= {
            val dn = elem \\ "DescriptorName"
            val mt = dn.filter(n=>(n \ "@MajorTopicYN").text == "Y")
            mt.map { n => n.text }
            
        }
        
        ////////start
        val mxml:RDD[Elem] = medline_raw.map(XML.loadString)
        val medline = mxml.map(majorTopics).cache
        medline.take(1).foreach(println) //List(Intellectual Disability, Maternal-Fetal Exchange, Pregnancy Complications)
        
        /**分析MeSH主要主题机器伴生关系*/
        println(medline.count) //30000
        val topics = medline.flatMap(mesh => mesh)
        val topicsCounts = topics.countByValue()
        println(topicsCounts.size) //7699
        
        val tcSeq = topicsCounts.toSeq 
        tcSeq.sortBy(_._2).reverse.take(10).foreach(println)
//(Disease,1202)
//(Neoplasms,983)
//(Tuberculosis,950)
//(Blood,518)
//(Anesthesia,379)
//(Wounds and Injuries,370)
//(Medicine,322)
//(Nervous System Physiological Phenomena,318)
//(Antibiotics, Antitubercular,298)
//(Body Fluids,284)        
        //对于引用次数的分布的统计
        val valueDist =tcSeq.groupBy(_._2).mapValues(_.size)
        valueDist.toSeq.sorted.take(10).foreach(println)
//(1,2347)
//(2,1136)
//(3,724)
//(4,487)
//(5,379)
//(6,307)
//(7,199)
//(8,193)
//(9,169)
//(10,141)        
        
        val topicPairs = medline.flatMap { t => t.sorted.combinations(2) } //主题之间两两组合
        val cooccurs = topicPairs.map(p=>(p,1)).reduceByKey(_+_)
        cooccurs.cache()
        cooccurs.count()
        
        val ord = Ordering.by[(Seq[String],Int), Int](_._2)
        cooccurs.top(10)(ord).foreach(println)
        //发现最常见的组合其实是出现最多主题个体之间的两两组合
//(List(Antibiotics, Antitubercular, Dermatologic Agents),195)
//(List(Analgesia, Anesthesia),181)
//(List(Analgesia, Anesthesia and Analgesia),179)
//(List(Anesthesia, Anesthesia and Analgesia),177)
//(List(Anesthesia, Anesthesiology),153)
//(List(Nervous System Physiological Phenomena, Reflex),151)
//(List(Body Fluids, Urine),149)
//(List(Antibodies, Antigens),133)
//(List(Niacin, Tuberculosis),131)
//(List(Isoniazid, Niacin),129)       
                
        val vertices =  topics.map { topic => (hashId(topic),topic) }   
        val uniqueHashes = vertices.map(_._1).countByValue() 
        val uniqueTopics = vertices.map(_._2).countByValue() 

        println(uniqueHashes.size == uniqueTopics.size)
        
        val edges = cooccurs.map(p=>{
          val (topics, cnt) = p
          val ids = topics.map(hashId).sorted
          Edge(ids(0),ids(1),cnt)
        })
        
        val topicGraph = Graph(vertices, edges)
        topicGraph.cache
        ///////////////////////未完  P119
    }
    
    def hashId(str:String) = {
        Hashing.md5().hashString(str).asLong()
    }
  
}