package person.tzg.scala.similarity

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import scala.collection.mutable.ArrayBuffer

/**
 * Created by arachis on 2016/11/22.
 * 相似度计算方法：计算两个商品共同出现的次数和各个商品出现次数的平方根的比值
 * 值越大，相似度越大
 *
 * 注：数据相似度矩阵要做标准化
 */
class CooccurenceSimilarity extends Similarity {

  /**
   * 计算物品共现矩阵
   * @param user_item 用户-物品评分矩阵
   * @return ((item1,item2),freq)
   */
  def calculateCoccurenceMatrix(user_item: RDD[(String, String)]): RDD[((String, String), Int)] = {

    val cooccurence_matrix = user_item.reduceByKey((item1,item2) => item1+"-"+item2 ).flatMap[((String, String), Int)](t2 => {
      val items = t2._2.split("-")
      var list = ArrayBuffer[((String, String), Int)]()
      for(i <- 0 to items.length-1){
        for(j <-i to items.length-1){
          if(items(i) != items(j)){
            val tuple = ( (items(j),items(i)), 1)
            list += tuple
          }
          val tuple2 = ( (items(i),items(j)), 1 )
          list += tuple2
        }
      }
      list
    }).reduceByKey((v1, v2) => v1 + v2)
    cooccurence_matrix
  }

  /**
   * 计算物品的相似度算法
   * @param sim_type 相似度计算方法
   * @return 物品i和物品j的相似度
   */
  def similarity(sim_type: String = "cosine_s", ratings: RDD[(String, String, Double)]): RDD[(String, String, Double)] = {

    //convert to user-item matrix
    val user_item: RDD[(String, String)] = ratings.map(t3 => (t3._1, t3._2)).sortByKey()
    user_item.cache()
    user_item.count()

    //calculate the cooccurence matrix of item
    val cooccurence_matrix: RDD[((String, String), Int)] = calculateCoccurenceMatrix(user_item)
    //cooccurence_matrix.saveAsTextFile("/user/tongzhenguo/recommend/cooccurence_matrix")

    val same_item_matrix: RDD[((String, String), Int)] = cooccurence_matrix.filter(t2 => {
      t2._1._1 == t2._1._2
    })

    val common_item_matrix: RDD[((String, String), Int)] = cooccurence_matrix.filter(t2 => {
      t2._1._1 != t2._1._2
    })

    //calculate the similarity of between item1 and item2
    val rdd_right: RDD[(String, Int)] = same_item_matrix.map(t2 => {

      val item: String = t2._1._1
      val n: Int = t2._2
      (item, n) //item,count
    })

    val rdd_left: RDD[(String, (String, String, Int, Int))] = common_item_matrix.map(t2 => {
      (t2._1._1, (t2._1._1, t2._1._2, t2._2)) //item1,item2,count
    }).join(rdd_right).map(t2 => {
      //item1,item2,f12,f2
      (t2._2._1._2, (t2._2._1._1, t2._2._1._2, t2._2._1._3, t2._2._2))
    })

    rdd_left.join(rdd_right).map(t2 => {
      val item_i = t2._2._1._1
      val item_j = t2._2._1._2
      val nij = t2._2._1._3
      val ni = t2._2._1._4
      val nj = t2._2._2

      (item_i, item_j, (1.0 * (10000 * nij / math.sqrt(ni * nj)).toInt / 10000)) //cosine_s
    })
  }

}