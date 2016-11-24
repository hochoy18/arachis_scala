package person.tzg.scala



import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import person.tzg.scala.common.CommonUtil
import person.tzg.scala.similarity.CosineSimilarity

/**
 * Created by arachis on 2016/11/21.
 * spark 实现基于物品的协同过滤
 * 参考：http://blog.csdn.net/sunbow0/article/details/42737541
 */
object ItemBaseCF extends Serializable {

  val conf = new SparkConf().setAppName(this.getClass.getName)
  val sc = new SparkContext(conf)
  val train_path: String = "/tmp/train.csv"
  val test_path = "/tmp/test.csv"
  val predict_path = "/user/tongzhenguo/recommend/cosine_s_predict"
  val split: String = ","

    /**
     * 预测用户对物品的评分
     * @param item_similarity 物品相似度
     * @param user_rating 用户评分数据
     * @return (user,(item_j,predict) )
     */
    def predict(item_similarity: RDD[(String, String, Double)], user_rating: RDD[(String, String, Double)]): RDD[(String, (String, Double))] = {

      //矩阵计算——i行与j列元素相乘
      val rdd_1 = item_similarity.map(t3 => (t3._2, (t3._1, t3._3))).join(user_rating.map(t3 => (t3._2, (t3._1, t3._3)))).map(t2 => {

        val item = t2._2._1._1
        val user = t2._2._2._1
        val wi: Double = t2._2._2._2
        val ri: Double = t2._2._1._2
        val weight = wi * ri
        ((user, item), 1.0 * (weight * 10000).toInt / 10000)
      })
    //矩阵计算——用户：元素累加求和
    val rdd_sum = rdd_1.reduceByKey(((v1,v2)=>v1+v2)).map(t2 => {

      val user = t2._1._1
      val item_j = t2._1._2
      val predict = t2._2
      (user,(item_j,predict) )
    })
    rdd_sum
  }

  /**
   * 预测用户对物品的评分
   * 数据量太大，提前过滤出相似度和评分高的数据
   * 也可以提前过滤出前k个物品
   * @param item_similarity 物品相似度
   * @param user_rating 用户评分数据
   * @param topK 保留前k个相似物品
   * @return (user,(item_j,predict) )
   */
  def predict(item_similarity: RDD[(String, String, Double)], user_rating: RDD[(String, String, Double)], topK: Int): RDD[(String, (String, Double))] = {
    //缩小相似度矩阵，只留下相似度最高的前k个
    val sorted_item_sim: RDD[(String, String, Double)] = item_similarity.map(t3 => {
      (t3._1, (t3._2, t3._3))
    }).groupByKey().map(f => {
      val i2 = f._2.toBuffer
      val i2_2 = i2.sortBy(_._2)
      if (i2_2.length > topK) {
        i2_2.remove(0, (i2_2.length - topK))
      }
      (f._1, i2_2.toIterable)
    }).flatMap(f => {
      val id2 = f._2
      for (w <- id2) yield (f._1, w._1, w._2)

    })

    //filter redundant (user,item,rating),this set user favorite (best-loved) 100 item
    val ratings = user_rating.groupBy(t3 => t3._1).flatMap(x => (
      x._2.toList.sortWith((x, y) => x._3 > y._3).take(100))
    )

    //矩阵计算——i行与j列元素相乘
    val rdd_1 = sorted_item_sim.map(t3 => (t3._2, (t3._1, t3._3))).join(ratings.map(t3 => (t3._2, (t3._1, t3._3)))).map(t2 => {

      val itemi = t2._2._1._1
      val user = t2._2._2._1
      val wi: Double = t2._2._2._2
      val ri: Double = t2._2._1._2
      val weight = wi * ri
      ((user, itemi), 1.0 * (weight * 10000).toInt / 10000)
    })
    //矩阵计算——用户：元素累加求和
    val rdd_sum = rdd_1.reduceByKey(((v1,v2)=>v1+v2)).map(t2 => {

      val user = t2._1._1
      val item_j = t2._1._2
      val predict = t2._2
      (user,(item_j,predict) )
    })
    rdd_sum
  }

  /**
   * 为用户推荐item
   * @param item_similarity 物品预测评分
   * @param user_rating 用户评分矩阵
   * @param r_number 推荐物品个数
   * @return （user,item,predict）
   */
  def recommend(item_similarity: RDD[(String, String, Double)], user_rating: RDD[(String, String, Double)],r_number:Int):RDD[(String, String, Double)] = {
    //缩小相似度矩阵，只留下相似度最高的前k个
    val sorted_item_sim: RDD[(String, String, Double)] = item_similarity.map(t3 => {
      (t3._1, (t3._2, t3._3))
    }).groupByKey().map(f => {
      val i2 = f._2.toBuffer
      val i2_2 = i2.sortBy(_._2)
      if (i2_2.length > r_number) {
        i2_2.remove(0, (i2_2.length - r_number))
      }
      (f._1, i2_2.toIterable)
    }).flatMap(f => {
      val id2 = f._2
      for (w <- id2) yield (f._1, w._1, w._2)

    })
    //filter redundant (user,item,rating),this set user favorite (best-loved) 100 item
    val ratings = user_rating.groupBy(t3 => t3._1).flatMap(x => (
      x._2.toList.sortWith((x, y) => x._3 > y._3).take(100))
    )
    //矩阵计算——i行与j列元素相乘
    val rdd_1 = sorted_item_sim.map(t3 => (t3._2, (t3._1, t3._3))).join(ratings.map(t3 => (t3._2, (t3._1, t3._3)))).map(t2 => {

      val itemi = t2._2._1._1
      val user = t2._2._2._1
      val wi: Double = t2._2._2._2
      val ri: Double = t2._2._1._2
      val weight = wi * ri
      ((user, itemi), 1.0 * (weight * 10000).toInt / 10000)
    })
    //矩阵计算——用户：元素累加求和
    val rdd_sum = rdd_1.reduceByKey(((v1,v2)=>v1+v2)).map(t2 => {

      val user = t2._1._1
      val item_j = t2._1._2
      val predict = t2._2
      (user,(item_j,predict) )
    })
    //矩阵计算——用户：用户对结果排序，过滤
    rdd_sum.groupByKey.map(f => {
      val i2 = f._2.toBuffer
      val i2_2 = i2.sortBy(_._2)
      if (i2_2.length > r_number)i2_2.remove(0,(i2_2.length-r_number))
      (f._1,i2_2.toIterable)
    }).flatMap(f=> {
      val id2 = f._2
      for (w <-id2) yield (f._1,w._1,w._2)

    })
  }

  def main(args: Array[String]) {

    //parse to user,item,rating 3tuple
    val ratings: RDD[(String, String, Double)] = CommonUtil.loadTrainData(sc,train_path)

    //calculate similarity
    val sim = CosineSimilarity
    val rdd_cosine_s: RDD[(String, String, Double)] = sim.similarity("cosine",ratings) //(8102,6688,0.006008038124054778)

    //recommend to user top n
    val recommend_ = recommend(rdd_cosine_s, ratings,10)

  }

}
