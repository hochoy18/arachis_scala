package org.apache.spark.fpg

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by arachis on 2016/12/16.
 */
object fpgrowthdemo extends Serializable {
  val conf = new SparkConf().setAppName("SimpleFPGrowth")
  val sc = new SparkContext(conf)

  def main(args: Array[String]) {
    val HDFS = "hdfs://192.168.1.222:8020"
    val music_genre_path: String = HDFS + "/user/tongzhenguo/data/music_genre"
    val data = sc.textFile(music_genre_path)

    val transactions: RDD[Array[String]] = data.map(s => s.trim.split(' '))

    transactions.cache()
    transactions.count()
    val fpg = new FPGrowth().setMinSupport(0.2).setNumPartitions(10)
    val model = fpg.run(transactions)

    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }

    val minConfidence = 0.7
    model.generateAssociationRules(minConfidence).collect().foreach { rule =>
      println(
        rule.antecedent.mkString("[", ",", "]")
          + " => " + rule.consequent.mkString("[", ",", "]")
          + ", " + rule.confidence)
    }


  }


}
