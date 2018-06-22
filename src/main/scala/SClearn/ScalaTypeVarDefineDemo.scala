package SClearn

/**
 * Created by arachis on 2016/12/23.
 *
 * 《快学scala》 17.3节 类型变量界定示例
 */
object ScalaTypeVarDefineDemo {

  /*  * 编译会出错，因为并不知道first是否有compareTo方法；需要添加一个上界 T<:Comparable[T],这意味着T必须是Comparable[T]的子类型
    class Pair(val first:T,val second:T){
      def smaller = if (first.compareTo(second) < 0) first else second
    }*/

  //通过这个限定，
  class Pair[T <: Comparable[T]](val first: T, val second: T) {
    def smaller = if (first.compareTo(second) < 0) first else second
  }

  val p = new Pair("Fred", "Brooks")
  println(p.smaller)


  /**
   * 也可以为类型指定一个下界。举例来讲，假定我们想要定义一个方法，用另一个值替换对偶的第一个组件
   */
/*  class Pair[T <: Comparable[T]](val first: T, val second: T) {
    //def repalceFirst(newFirst:T) = new Pair[T](newFirst,second)
    //假定我们有一个Pair[Student],我们应该允许用一个Person来替换第一个组件。替换进来的类型必须是原类型的超类型
    def repalceFirst[R >: T](newFirst: R) = new Pair[R](newFirst, second)
  }*/


}



























