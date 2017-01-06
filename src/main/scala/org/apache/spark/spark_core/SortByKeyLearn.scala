package org.apache.spark.spark_core

import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}

/**
 * Created by arachis on 2016/12/20.
 */
class SortByKeyLearn extends Serializable {
  /**
   * https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/rdd/OrderedRDDFunctions.scala
   */
  val sc = new SparkContext(new SparkConf)

  def main(args: Array[String]) {

    sc.parallelize(Seq("A", "V", "B", "V", "C", "V", "W")).map { s => ((s, 1), 1) }.sortByKey()
    //res13: Array[((String, Int), Nothing)] = Array(((W,1),1), ((V,1),1), ((V,1),1), ((V,1),1), ((C,1),1), ((A,1),1), ((B,1),1))

    val pairs = sc.parallelize(Seq("A", "V", "B", "V", "C", "V", "W")).map { s => ((s, 1), 1) }

    import org.apache.spark.rdd.ShuffledRDD

    val part = new RangePartitioner(5, pairs, false)
    implicit val ordering = new Ordering[(String, Int)] {
      override def compare(a: (String, Int), b: (String, Int)) = {
        if ((a._1.compare(b._1)) == 0) {
          (a._2 - b._2)
        } else {
          a._1.compare(b._1)
        }
      }

      val rdd2 = new ShuffledRDD(pairs, part)
      rdd2.collect()
      //res13: Array[((String, Int), Nothing)] = Array(((W,1),1), ((V,1),1), ((V,1),1), ((V,1),1), ((C,1),1), ((A,1),1), ((B,1),1))
    }

  }
}