package person.tzg.scala.similarity

import org.apache.spark.rdd.RDD

/**
 * Created by arachis on 2016/11/22.
 * 相似度计算方法：计算两个向量的夹角余弦值
 * 优化：
 *  只计算半相似度矩阵
 *  考虑用户过多评分数据，过滤
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
    if( !"cosine".equals(sim_type) ){
      return null
    }
    val user_row = ratings.map(t3 => {
      val user = t3._1
      val item = t3._2
      val rating = t3._3
      (user, (item, rating))
    })

    //user key join user key
    val sim = user_row.join(user_row).map(t2 => {
      val item1 = t2._2._1._1
      val item2 = t2._2._2._1
      val r1 = t2._2._1._2
      val r2 = t2._2._2._2
      ((item1, item2), (r1 * r1, r2 * r2, r1 * r2))
    }).filter(t2 => {
      t2._1._1 < t2._1._2
    }).reduceByKey((v1_t3, v2_t3) => {
      val sum_ii: Double = v1_t3._1 + v2_t3._1
      val sum_jj: Double = v1_t3._2 + v2_t3._2
      val sum_ij: Double = v1_t3._3 + v2_t3._3
      (sum_ii, sum_jj, sum_ij)
    }).map(t2 => {
      val sum_ij = t2._2._3
      val sum_jj = t2._2._2
      val sum_ii = t2._2._1
      val cosine = sum_ij / math.sqrt(sum_ii * sum_jj)
      (t2._1._1,t2._1._2, 1.0 * (cosine * 10000).toInt / 10000)
    })
    //矩阵反转，没有相同物品的相似度
    val union = sim.map(t3 => {
      (t3._1._2, t._1._1, t._1._3)
    }).union(sim)
    union
  }
}
