package demo.scala.study

class Rational(n:Int, d:Int) {  //不能直接使用that.n  that.d
    require(d!=0) //分母不为0
    println("Created "+n+"/"+d)
    private val g = gcd(n.abs, d.abs)
    val number:Int = n/g
    val denom:Int = d/g
    def this(n:Int) = this(n,1)
    override def toString=number+"/"+denom 
    def add(that:Rational):Rational=
        //new Rational(n*that.d+that.n*d,d*that.d) //错误，无法直接使用that.n  that.d
        new Rational(number*that.denom+that.number*denom,
                        d*that.denom)
    
    def lessThan(that:Rational)= 
        this.number*that.denom < that.number*this.denom
        
    def max(that:Rational)=
        if(this.lessThan(that)) that else this
        
    private def gcd(a:Int, b:Int):Int=
        if(b==0) a else gcd(b, a%b)
        
    def +(that:Rational):Rational=add(that)
    def +(i:Int)=new Rational(number+i*denom, denom) //方法重载
    def -(that:Rational):Rational=
        new Rational(number*that.denom-that.number*denom,
                denom*that.denom)
    def -(i:Int):Rational=
        new Rational(number - i*denom, denom)
    
    def *(that:Rational)=
        new Rational(number*that.number, denom*that.denom)
    
    def *(i:Int) = 
        new Rational(number*i,denom)
    def /(that:Rational)=
        new Rational(number*that.denom, denom*that.number)
    def /(i:Int)=
        new Rational(number, denom*i)
    
    
}

object ScalaDemo21 {
    def main(args: Array[String]): Unit = {
        val four = new Rational(4) // 4/1
        val oneHalf = new Rational(1,2)
        println(oneHalf)
        val twoThirds = new Rational(2,3)
        val re = oneHalf add twoThirds
        println(re) // 7/6
        println(re.number)    //7
        println(re.denom)    //6
        println(new Rational(66, 42)) // 11/7
        println(new Rational(1,2) + new Rational(1,2)) //1/1
        val x = new Rational(1,2)        
        println(x*2) //1/1
        
        implicit def intToRational(x:Int)= new Rational(x)
        println(2*x)    //1/1
        
        
        
    }
  
}
	