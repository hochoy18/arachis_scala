package learntoscala

/**
 * Created by arachis on 2017/1/6.
 * 闭包（closure）：
 *    依照这个函数文本在运行时创建的函数值（对象）被称为闭包：closure;名称源自于通过“捕获”自由变量的绑定对函数文本执行的“关闭”行动。
 *    自由变量：free variable
 *    绑定变量：bound variable
 *    封闭术语：closed term
 *    开放术语：open term
 *
 *
 */
object closure {
  def main(args: Array[String]) {
      // (x: Int) => x + more  这里x是函数参数，也叫绑定变量，因为其在函数的上下文中有明确的含义，一个int;而more 是自由变量，因为函数上下文没有定义这个变量

      val more = 1
      val addMore = (x:Int) => x + more
      print(addMore(10))
  }

}
