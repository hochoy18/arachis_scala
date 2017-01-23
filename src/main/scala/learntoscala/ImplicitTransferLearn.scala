package learntoscala

/**
 * Created by arachis on 2017/1/23.
 *
 * 优雅的使用别人的代码或者扩展你的代码
 *  隐式转换规则：
 *    标记：只有标记为implicit的定义才是可用的
 *    作用域：隐式转换必须处于作用域内或者和转换源关联在一起（引用）
 *    无歧义：不能有两个对同一转换源的操作
 *    单一调用规则：只会使用同一隐式转换
 *    显示优先：如果类型检查无误，则不会隐式转换
 *    命名隐式转换：
 */
object ImplicitTransferLearn extends Serializable{
  implicit def intToString(x:Int) = x.toString
  implicit def stringToInt(x:String) = x.toInt

  def main(args: Array[String]) {

    val s = "1"
    println(1-s)


  }

}
