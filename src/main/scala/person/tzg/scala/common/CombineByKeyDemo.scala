package person.tzg.scala.common

import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by arachis on 2016/11/14.
 */
object CombineByKeyDemo extends Serializable {
  val sc = new SparkContext(new SparkConf())

  def main(args: Array[String]) {
    //count
    sc.parallelize(Seq(1, 2, 4, 3, 5, 6, 7, 1, 2)).map(i => (i, 1)).combineByKey(
      x => x
      , (v1: Int, v2: Int) => v1 + v2
      , (v1: Int, v2: Int) => v1 + v2
    )

  }

}
