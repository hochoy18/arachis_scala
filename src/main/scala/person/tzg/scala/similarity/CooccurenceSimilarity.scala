package person.tzg.scala.similarity

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

/**
 * Created by arachis on 2016/11/22.
 * 相似度计算方法：计算两个商品共同出现的次数和各个商品出现次数的平方根的比值
 * 值越大，相似度越大
 *
 * 注：假定已经对数据做了标准化
 */
class CooccurenceSimilarity extends Similarity{

  /**
   * 计算物品的相似度算法
   * @param sim_type 相似度计算方法
   * @return 物品i和物品j的相似度
   */
  override def similarity(sim_type: String, ratings: RDD[(String, String, Double)]): RDD[(String, String, Double)] = {

    //convert to user-item matrix
    val user_itemRatring: RDD[(String, (String, Double))] = ratings.map(t3 =>  (t3._1,(t3._2, t3._3)) ).sortByKey()
    user_itemRatring.cache()
    user_itemRatring.count()

    //calculate consine value
    val item_value: RDD[(String, (Double, Double))] = user_itemRatring.map(t2 => {
      val item: String = t2._2._1
      val r: Double = t2._2._2
      (item, (r, r * r))
    }).reduceByKey((v1_t2, v2_t2) => (v1_t2._1 + v2_t2._1, v1_t2._2 + v2_t2._2))

    item_value.join(item_value).map(t2 =>{

  })