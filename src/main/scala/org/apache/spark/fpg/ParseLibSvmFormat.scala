package org.apache.spark.fpg

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by arachis on 2016/12/16.
 */
object ParseLibSvmFormat extends Serializable {
  val sc = new SparkContext(new SparkConf())

  def main(args: Array[String]) {
    val HDFS = "hdfs://192.168.1.222:8020"
    val music_genre_path: String = HDFS + "/user/tongzhenguo/data/music_genre"
    val header = "/user/tongzhenguo/data/userage_musicgenre_dataset_libsvm"
    val month = "20160701-20160801"

    sc.textFile(HDFS + header + "/" + month).map(s => {
      val transaction = s.replaceAll("\\(", "").replaceAll("\\)", "").replaceAll(",", " ").replaceAll(":[0-9]{1,10} ", " ")
      transaction
    }).filter(s => {
      val ss: Array[String] = s.split(" ")
      val uniq = ss.toSet
      uniq.size == ss.length
    }).repartition(100).saveAsTextFile(music_genre_path)

  }

}
