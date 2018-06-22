package SClearn

import scala.collection.mutable

/**
  *
  * Created by tongzhenguo on 2018/6/22.
  * scala Map用例
  *
  */
object MapUserCase {

  def main(args: Array[String]): Unit = {
    // 可变映射初始化方法
    val muMap = mutable.Map("arachis" -> 888, ("zhl", 520))
    val muEmptyMap = new mutable.HashMap[String, Int]

    println(muMap.mkString(","))
    // arachis -> 888,zhl -> 520
    println(muEmptyMap.mkString(","))

    // 更新映射
    muMap.put("ara", 20180622)
    muEmptyMap("arachis") = 1000
    println(muMap.mkString(","))
    // arachis -> 888,zhl -> 520,ara -> 20180622
    println(muEmptyMap.mkString(","))
    // arachis -> 1000

    // 删除元素
    muEmptyMap.remove("arachis")
    muMap -= "arachis"
    println(muMap.mkString(","))
    // zhl -> 520,ara -> 20180622
    println(muEmptyMap.mkString(","))
    //

    // 遍历元素
    for ((key, value) <- muMap) {
      println(key, value)
    }

    /**
      * (zhl,520)
      * (ara,20180622)
      */

    // 与元组的互相转化
      val array: Array[(String, Int)] = muMap.toArray
      val tuples: Array[(String, Int)] = Array("a", "b").zip(Array(1, 2))
      println(array.mkString(","))
      // (zhl,520),(ara,20180622)
      println(tuples.mkString(","))
      // (a,1),(b,2)

  }
}
