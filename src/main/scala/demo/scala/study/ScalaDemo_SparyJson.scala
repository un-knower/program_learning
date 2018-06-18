package demo.scala.study

/**
 * https://github.com/spray/spray-json#providing-jsonformats-for-other-types
 * 		   parseJson            convertTo[T]
 * String ----------> JSON AST --------------> Scala types:T
 * 
 * https://github.com/jestan/spray-json-examples
 */
import spray.json._
object ScalaDemo_SparyJson {
    def main(args:Array[String]):Unit = {
        case class Color(name:String, red:Int, green:Int, blue:Int)
        object MyJsonProtocol extends DefaultJsonProtocol {
            implicit val colorFormat = jsonFormat4(Color) //4个属性
        }
        import MyJsonProtocol._ 
        //object -> json
        val json = Color("CadetBlue",95,158,160).toJson
        println(json) //{"name":"CadetBlue","red":95,"green":158,"blue":160}
        println(json.prettyPrint)
        //json -> object
        val color = json.convertTo[Color]
        println("name:"+color.name+",red:"+color.red+",green:"+color.green+",blue:"+color.blue) //name:CadetBlue,red:95,green:158,blue:160
        
        val jsons = "[{\"name\":\"CadetBlue\",\"red\":95,\"green\":158,\"blue\":160},{\"name\":\"CadetRed\",\"red\":160,\"green\":158,\"blue\":95}]"
        //string -> json -> object
        val colors:List[Color] = jsons.parseJson.convertTo(DefaultJsonProtocol.listFormat[Color])
        colors.foreach { color => 
            println("name:"+color.name+",red:"+color.red+",green:"+color.green+",blue:"+color.blue)
            
        }
        // object -> json
        val listjson = colors.toArray.toJson
        
        //list -> rdd
        //val colorsRdd = sc.parallelize(colors) 
        
        
        
        
        
        
    }
  
}