package org.apache.spark.spark_core

import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}

/**
 * Created by arachis on 2016/12/20.
 */
object RangePartioner extends Serializable {

  val sc = new SparkContext(new SparkConf)

  def main(args: Array[String]) {
    val pairs = sc.parallelize(Seq("A", "V", "B", "V", "C", "V", "W")).map((_, 1)).sortByKey()
    pairs.partitioner
    //res10: Option[org.apache.spark.Partitioner] = Some(org.apache.spark.RangePartitioner@a79311ed)

    val part = new RangePartitioner(5, pairs, false)
    part.getPartition("A")
    //res4: Int = 4

    scala > part.getPartition("W")
    //res6: Int = 1


  }


}
