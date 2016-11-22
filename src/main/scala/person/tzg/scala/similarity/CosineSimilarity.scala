package person.tzg.scala.similarity

import org.apache.spark.rdd.RDD

/**
 * Created by arachis on 2016/11/22.
 * 相似度计算方法：计算两个向量的夹角余弦值
 * 值越大，相似度越小
 *
 * 注：假定已经对数据做了标准化
 */
class CosineSimilarity extends Similarity{
  /**
   * 计算物品的相似度算法
   * @param sim_type 相似度计算方法
   * @return 物品i和物品j的相似度
   */
  override def similarity(sim_type: String, ratings: RDD[(String, String, Double)]): RDD[(String, String, Double)] = {




  }
}
