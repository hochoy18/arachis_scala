package learntoscala

/**
 * Created by arachis on 2017/1/23.
 *
 * 《Scala 编程》 第16章 使用列表
 *  List 是不可变的
 *
 */
object ListLearn extends Serializable{

  def main(args: Array[String]) {

    /**
     * 创建列表
     */
//    val fruit = "apples"::("oranges"::("pears"::Nil))//列表由Nil和::(读作“cons”)构成，前者代表空列表，后者代表列表从前端扩展
//    val fru = List[String]("apples","oranges","pears")
//    println("fruit:"+fruit+" fru:"+fru) //两者等价
//    val empty: List[Nothing] = List.empty //创建一个空的列表
//    List.range(1,10,2)//创建等差数列
//    val ones: List[Int] = List.fill(10)(1) //创建n个初始值为1的列表，这里fill是一个闭包
//    println(ones)


    /**
     * 列表基本操作：数组访问
     */
    val nums: List[Int] = List(1, 2, 3, 4)
//    println(nums.isEmpty) //false
//    println(nums.length) //判断为空时不建议使用nums.length == 0

//    println(nums.head) //取第1个元素
//    println(nums take 2) //取前n个元素
//    println(nums.tail) //List(2, 3, 4),这里要注意下
//    println(nums.last) //取最后一个元素
//    println(nums drop 2) //取最后n个元素
//    println(nums.init) //List(1, 2, 3)

    /**
     * 列表操作：
     *  连接
     *  反转
     *  排序
     *  下标操作
     */
//    List.concat(nums,nums)
//    println(nums.reverse) //List不是原地的，而是返回一个新列表
//    println(nums.sorted)
//
//    println(nums.indices) //List的所有下标
//    println(nums.zip(nums.indices))
//    println(nums.zipWithIndex)

    /**
     * 显示列表
     */
//    println(nums.toString)
//    println(nums.mkString("[",",","]")) //参数：指定前缀，分隔符，后缀；python风格的列表


    /**
     * 集合类型相互转换
     */
//    nums.toArray
//    nums.toSeq
//    nums.toSet
//    val iterator: Iterator[Int] = nums.toIterator
//    val iterator1: Iterator[Int] = nums.iterator

    /**
     * 分布式操作
     */
//    nums.map(n=>{
//      println( n+1)
//      n+1
//    })
//    println( nums.flatMap( n => List.range(1,n+1) ) ) //将集合类型合成一个大集合
//    println( nums.reduce((v1,v2)=>v1+v2)  )
//    println(nums.filter(_ > 2)) //返回满足条件的所有
//    println(nums.find(_ > 2)) //返回满足条件的第一个
//    println(nums.partition(n => n % 2 == 0)) //将偶数作为一个分区
//    nums.foreach(_ => print(_+","))


    /**
     * 列表模式
     * 使用模式匹配进行拆分
     */
//    val List(a,b,c) = fru
//    println(fru)
  }

}
