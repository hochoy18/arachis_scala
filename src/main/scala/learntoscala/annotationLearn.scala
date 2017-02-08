package learntoscala

import scala.collection.mutable

/**
 * Created by arachis on 2016/12/24.
 * <<快学scala>> 第十五章注解
 * 15.5 针对Java特性的注解
 *  15.5.1 Java 修饰符
 *
 */
object annotationLearn {
    /*
    *   对于那些不是很常用的Java特性，Scala使用注解，而不是修饰符关键字。
    */

  //@transient 注解将字段标记为瞬态的：
  @transient var recentLookups = new mutable.HashMap[String,String]
  /*
  * 瞬态的字段不会被序列化。这对于需要临时保存的缓存数据，或者是能够很容易地重新计算的数据而言是合理的。
  */






















}
