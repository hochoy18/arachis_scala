package org.apache.spark

import org.apache.spark.rdd.RDD

/**
 * Created by arachis on 2016/11/22.
 * :因为sc.checkpointFile(path)是private[spark]的，所以该类要写在自己工程里新建的package org.apache.spark中
 */
object RDDUtils extends SparkContext with Serializable{
  def getCheckpointRDD[T](sc:SparkContext, path:String) = {
    //path要到part-000000的父目录
    val result : RDD[Any] = sc.checkpointFile(path)
    result.asInstanceOf[T]
  }

}
