package demo.spark.mllib.component

import scopt.OptionParser
import scopt.OptionParser

object OptionParserDemo {
    case class Params(
        input:String = null,
        output:String = null,
        k:Int = -1,
        numIterations:Int = 10,
        opt:Boolean = false
    )
    
    def run(params: Params) {
        println(params.input)
        println(params.output)
        println(params.k)
        println(params.numIterations)
        println(params.opt)
    }
    
    def main(args: Array[String]): Unit = {
        val defaultParams = Params() //默认参数
        
        val parser = new OptionParser[Params]("programName") {
            head("programName","version 3") // adds usage text. 
              
            opt[String]('i',"input")   //`-i value` or `--input value`.
                .required() //Requires the option to appear at least once
                .action((x,c)=>c.copy(input=x)) ////Adds a callback function. 
                .text("input is a required input file path property.default is "+defaultParams.input) //Adds description in the usage text.
            
            opt[String]('o',"output")
                .required()
                .action((x,c)=>c.copy(output=x))
                .text("output is a required result output path property.default is "+defaultParams.output)
            
            opt[Int]('k',"k")
                .action((x,c)=>c.copy(k=x))
                .text("k is a optional clusters num property.default is "+defaultParams.k)
                
            opt[Int]('n',"numIterations")
                .action((x,c)=>c.copy(numIterations=x))
                .text("k is a optional num of iteration property.default is "+defaultParams.numIterations)
                .validate { x =>  //Adds custom validation
                    if(x>=10) success 
                    else failure("Value <numIteration> must be >= 10")
                 }
            opt[Boolean]('p',"opt")
                .action((x,c)=>c.copy(opt=x))
                .text("opt is a optional property.default is "+defaultParams.opt)
            
            //help("help").text("prints this usage text") //加上，如果参数有误，只会报出错误语句，而不会自动显示使用用法。去掉该句，参数错误会自动显示使用方法
            /*  arg 不使用参数，直接跟输入即可
            arg[String]("<input>")
                .text("input paths to examples")
                .required()
                .action((x, c) => c.copy(input = x))
            */    
        }
        parser.parse(args,defaultParams).map {  params =>
                run(params)
        }.getOrElse {
              println("error options")
              sys.exit(1)
        }
          
    }
}