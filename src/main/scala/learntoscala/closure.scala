package learntoscala

/**
 * Created by arachis on 2017/1/6.
 * 闭包（closure）：
 *    依照这个函数文本在运行时创建的函数值（对象）被称为闭包：closure;名称源自于通过“捕获”自由变量的绑定对函数文本执行的“关闭”行动。
 *    自由变量/捕获变量：free variable
 *    绑定变量：bound variable
 *    封闭术语：closed term
 *    开放术语：open term
 *
 */
object closure {
  def main(args: Array[String]) {
      // (x: Int) => x + more  这里x是函数参数，也叫绑定变量，因为其在函数的上下文中有明确的含义，一个int;而more 是自由变量，因为函数上下文没有定义这个变量

      val more = 1
      val addMore = (x:Int) => x + more //这是一个开放术语，即其带有自由变量
      val addMode = (x:Int,y:Int) => x+y //相对的这是一个封闭术语，即其不带任何自由变量
      print(addMore(10))

    val someNumbers = List(-11, -10, -5, 0, 5, 10)
    var sum = 0
    someNumbers.foreach(sum += _) //闭包看到了闭包之外捕获变量的变化；反过来，闭包对捕获变量做出的改变在闭包之外
    print(sum)

    def mulBy(factor:Double) = (x:Double) => factor * x
    val triple = mulBy(3)
    val half = mulBy(0.5)
    println(triple(14)+" "+half(14)) //42 7

    /**
     * 慢动作回放：
     *  mulBy首次调用将参数变量factor设为3。该变量在(x:Double) => factor * x 函数的函数体内被引用，该函数被存入triple。然后参数变量factor从运行时的栈上被弹出
     *  接下来，mulBy再次调用，这次factor设为0.5。该变量在(x:Double) => factor * x 函数的函数体内被引用，该函数被存入half。
     */
    //这些函数实际上是以类的对象方式实现的，该类有一个实例变量factor和一个包含了函数体放入apply方法
  }

}
