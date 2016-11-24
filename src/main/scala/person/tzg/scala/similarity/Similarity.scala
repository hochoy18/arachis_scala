package person.tzg.scala.similarity

import org.apache.spark.rdd.RDD

/**
 * Created by arachis on 2016/11/22.
 * 抽象相似度或相似距离的计算
 */
abstract class Similarity extends Serializable{
  /**
   * 计算物品的相似度算法
   * @param sim_type 相似度计算方法
   * @return 物品i和物品j的相似度
   */
  def similarity(sim_type: String = "cosine_s",ratings: RDD[(String, String, Double)]): RDD[(String, String, Double)]

}
