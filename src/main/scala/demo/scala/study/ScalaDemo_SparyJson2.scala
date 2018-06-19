package demo.scala.study

/**
 * https://github.com/spray/spray-json#providing-jsonformats-for-other-types
 * 		   parseJson            convertTo[T]
 * String ----------> JSON AST --------------> Scala types:T
 * 
 * https://github.com/jestan/spray-json-examples
 */
import spray.json._
object ScalaDemo_SparyJson2 {
    def main(args: Array[String]): Unit = {
        object EnumSex extends Enumeration { //性别枚举类型
            type Sex = Value
            val MALE = Value("MALE")
            val FeMALE = Value("FEMALE")
        }
        
        case class Address(no:String, street:String, city:String)
        case class Person(name:String, age:Int, sex:EnumSex.Sex, address:Address)
        object MyJsonProtocol extends DefaultJsonProtocol {
            implicit val addressFormat = jsonFormat(Address,"no","street","city") //对case class AddressDe 解析 
            implicit val sexJsonFormat = new JsonFormat[EnumSex.Sex] {
                def write(sex: EnumSex.Sex) =  JsObject("sex"->JsNumber(sex.id))
                def read(value:JsValue) = value match {
                    case JsNumber(sex) => EnumSex(sex.toInt)
                    case _ => throw new DeserializationException("person.sex expected")
                }
            }
            implicit val personFormat = jsonFormat(Person, "name", "age", "sex", "address")
        }
        
        import MyJsonProtocol._
        //对address string json的解析
        val jsonStr = """{ "no": "A1", "street" : "Main Street", "city" : "Colombo" }""" 
        val addressObj = JsonParser(jsonStr).fromJson[Address]
        //或者
   		val addressObj2 = jsonStr.parseJson.convertTo[Address]
        println(addressObj.city)
        println(addressObj2.city)
        
        
        val json2 = """{ "name" : "John", "age" : 26,  "sex" : 0 , "address" : { "no": "A1", "street" : "Main Street", "city" : "Colombo" }}"""
        val person = json2.parseJson.convertTo[Person]
        println(json2.parseJson.prettyPrint)
        println(person) //Person(John,26,MALE,Address(A1,Main Street,Colombo))
        println(person.name) //John
        println(person.sex) //MALE 这里是MALE，而不是0
        println(person.address.no) //A1
      
    }
  
}