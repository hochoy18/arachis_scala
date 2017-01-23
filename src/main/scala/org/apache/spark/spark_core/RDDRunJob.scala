package org.apache.spark.spark_core

import org.apache.spark.{SparkConf, SparkContext}


object RDDRunJob extends Serializable {
  val sc = new SparkContext(new SparkConf())

  def main(args: Array[String]) {
    sc.makeRDD(Seq("arachis", "tony", "lily", "tom")).map {
      name => (name.charAt(0), name)
    }.groupByKey().mapValues {
      names => names.toSet.size //unique and count
    }.collect()


  }

}
