package SClearn

/**
  *
  * Created by tongzhenguo on 2018/6/7.
  *
  * scala中"_"的使用案例：
  *   1.函数签名中使用"_"代表一个空参数
  *   2.匿名函数可以使用"_"代替一个参数
  *   3._作为通配符
  *   4._作为占位符
  *   5._*告诉编译器将集合里的元素作为可变参数传入
  *
  */
object underLineUseCase {

  def main(args: Array[String]): Unit = {

    // 在Scala中方法不是值，而函数是。所以一个方法不能赋值给一个val变量，而函数可以
    def increment(n: Int) = n + 1
    def add2(a:Int,b:Int):Int = a + b

    //    val fun = increment
    """
      |报错：error: missing argument list for method increment
      |Unapplied methods are only converted to functions when a function type is expected.
      |You can make this conversion explicit by writing `increment _` or `increment(_)` instead of `increment`.
      |       val fun = increment
    """.stripMargin

    // 1.函数签名中使用"_"代表一个空参数
    // 函数即使没有参数,也要使用"_"代表一个空参数
    val fun = increment(_)
    val fun2 = add2(_,_)
    println("%s,%s".format(fun,fun2))

    // 2.匿名函数可以使用"_"代替一个参数
    println((1 to 9).filter(_ % 2 == 0))
    println(List(10, 5, 8, 1, 7).sortWith(_ < _))

    // 3.作为通配符
    // 引入math包下的全部类
    import scala.math._
    // 使用_1代表元祖的第一个值
    val t = (1, 3.14, "Fred")
    println("t = (%s,%s,%s)".format(t._1,t._2,t._3))

    // 4.作为占位符

    // t = (1,3.14,Fred)
    // 当不需要元祖中某个值时可以用_起占位符作用
    val (first, second, _) = t
    // 类继承中默认id使用_id方法作为setter()
//    abstract class Person {
//      def id: Int
//    }
//    class Student extends Person{
//      override var id = 9527  //Error: method id_= overrides nothing
//    }
    """
      |error: method id_= overrides nothing
      |             override var id = 9527  //Error: method id_= overrides nothing
    """.stripMargin
    abstract class Person {
      def id: Int
      def id_=(value: Int) //父类必须有set方法
    }
    class Student extends Person{
      override var id = 9527 //为var变量自动生成get和set方法
    }

    // 5._*告诉编译器将集合里的元素作为可变参数传入
    def sum(n:Int*): Int ={
      var ret = 0
      for(v <- n) ret += v
      ret
    }
    println(sum(1 to 3:_*))
    // 当不需要数组中某几个值时可以用_*起占位符作用
    val Array(one, two, _*) = Array(1,2,3,4)

  }


}
