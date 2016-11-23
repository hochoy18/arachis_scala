package person.tzg.scala.predict

/**
 * Created by arachis on 2016/11/24.
 * 基于物品的协同过滤的评分预测算法
 */
object ItemBaseCF extends Serializable{
  /**
   * 预测用户对物品的评分
   * @param item_similarity 物品相似度
   * @param user_rating 用户评分数据
   * @return (user,(item_j,predict) )
   */
  def predict(item_similarity: RDD[(String, String, Double)], user_rating: RDD[(String, String, Double)]): RDD[(String, (String, Double))] = {
    //计算物品评分均值
    val rdd_item_mean = user_rating.map(t3 => (t3._2,(t3._3,1))).reduceByKey((v1,v2)=>( v1._1+v2_.1 , v1._2+v2._2 )).map(t2 =>{
      val item = t2._1
      val mean = ( t2._2._1 / t2._2._2 )
      (item,1.0 * (mean * 10000).toInt / 10000)
    })
    //计算物品差值
    val rdd_item_diff = user_rating.map(t3 => (t3._2, (t3._1, t3._3))).join(rdd_item_mean).map(t2 =>{
      val user = t2._1._1
      val item = t2._1
      val diff = t2._1._2 - t2._2._1
      (item,(user,diff))
    })

    //矩阵计算——i行与j列元素相乘
    val rdd_1 = item_similarity.map(t3 => (t3._2, (t3._1, t3._3))).join(rdd_item_diff).map(t2 => {

      val item = t2._2._1._1
      val user = t2._2._2._1
      val wi: Double = t2._2._2._2
      val r_diff: Double = t2._2._1._2
      val weight = wi * r_diff
      val fenzi: Double = 1.0 * (weight * 10000).toInt / 10000
      val fenmu: Double = 1.0 * (wi * 10000).toInt / 10000
      ( (user, item), (fenzi,fenmu) )
    })
    //矩阵计算——用户：元素累加求和
    val rdd_sum = rdd_1.reduceByKey(((v1,v2)=>v1+v2)).map(t2 => {

      val user = t2._1._1
      val item_j = t2._1._2
      val predict = ( t2._2._1 / t2._2._1 )
      (item_j,(user,predict) )
    }).join(rdd_item_mean).map(t2 =>{
      val item = t2._1
      val user = t2._2._1._1
      val predict = t2._2._2 + t2._2._1._2
      (user,(item,predict))
    })
    rdd_sum
  }

}
