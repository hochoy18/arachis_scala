package org.apache.spark.fpg

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by arachis on 2016/12/16.
 */
object formatcsv extends Serializable {
  val sc = new SparkContext(new SparkConf())

  def main(args: Array[String]) {
    val HDFS = "hdfs://192.168.1.222:8020"

    sc.textFile(HDFS + "/tmp/loan.csv").map(s => {
      val ss = s.split("\\t")
      val headers = "age\twork\thouse\tcredit\tlabel"
      val headerArr: Array[String] = headers.split("\\t")
      var resArr = headerArr
      for (i <- Seq(headerArr.length - 1)) {
        resArr(i) = headerArr(i) + ":" + ss(i)
      }
      resArr(0) + " " + resArr(1) + " " + resArr(2) + " " + resArr(3) + " " + resArr(4)
    })
  }

}
