package SClearn

/**
  *
  * Created by tongzhenguo on 2018/6/22.
  * 使用$插入变量值,优化代码可读性
  * [字符串插值](https://docs.scala-lang.org/zh-cn/overviews/core/string-interpolation.html)
  */
object Var$UserCase {

  def main(args: Array[String]): Unit = {

    val signl = 10086
    // 原来的写法
    println("signl = %s".format(signl))
    println("signl = val".replaceAll("val",signl.toString))

    // 优化写法
    println(s"signl = $signl")
    // 表达式写法
    println(s"signl = ${signl.toDouble}")
    // 格式化写法
    println(f"signl = ${signl.toDouble}%2.2f")
    // raw写法,不用转义
    println(raw"signl = ${if(signl > 1000) "hot" else "cold"}")
  }


}
