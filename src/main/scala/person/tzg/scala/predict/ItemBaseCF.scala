package person.tzg.scala.predict

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import person.tzg.scala.common.CommonUtil
import person.tzg.scala.similarity.CosineSimilarity

import scala.collection.Map

/**
 * Created by arachis on 2016/11/24.
 * 基于物品的协同过滤的评分预测算法
 */
object ItemBaseCF extends Serializable{

  val conf = new SparkConf().setAppName(this.getClass.getName)
  val sc = new SparkContext(conf)
  val train_path: String = "/tmp/train.csv"
  val test_path = "/tmp/test.csv"
  val predict_path = "/user/tongzhenguo/recommend/cosine_predict"
  val split: String = ","

  def main(args: Array[String]) {

    //parse to user,item,rating 3tuple
    val ratings: RDD[(String, String, Double)] = CommonUtil.loadTrainData(sc,train_path)

    //calculate similarity
    val sim = CosineSimilarity
    val rdd_cosine_s: RDD[(String, String, Double)] = sim.similarity("cosine",ratings) //(8102,6688,0.006008038124054778)

    //predict item rating
    val predict_ = predict(rdd_cosine_s, ratings,sc)
  }


  /**
   * 预测用户对物品的评分
   * @param item_similarity 物品相似度
   * @param user_rating 用户评分数据
   * @return (user,(item_j,predict) )
   */
  def predict(item_similarity: RDD[(String, String, Double)], user_rating: RDD[(String, String, Double)],sc:SparkContext): RDD[(String, (String, Double))] = {
    //计算物品评分均值
    val rdd_item_mean = CommonUtil.getMean(user_rating)
    val item_mean_map = rdd_item_mean.collectAsMap()
    val broadcast: Broadcast[Map[String, Double]] = sc.broadcast(item_mean_map)
    
    //计算物品差值
    val rdd_item_diff = user_rating.map(t3 => (t3._2, (t3._1, t3._3))).map(t2 =>{
      val item = t2._1
      val user = t2._2._1
      val r: Double = t2._2._2
      var mean = 3.4952
      if(None != broadcast.value.get(item)){
         mean = broadcast.value.get(item).get
      }
      val diff = r - mean
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
    val rdd_sum = rdd_1.reduceByKey(((v1_t2,v2_t2)=>{
      val sum_fenzi: Double = v1_t2._1+v2_t2._1
      val sum_fenmu: Double = v1_t2._2+v2_t2._2
      (sum_fenzi,sum_fenmu)
    })).map(t2 => {

      val user = t2._1._1
      val item = t2._1._2
      var mean = 3.4952
      if(None != broadcast.value.get(item)){
        mean = broadcast.value.get(item).get
      }
      val fenzi: Double = t2._2._1
      val fenmu: Double = t2._2._2
      val predict = mean+( fenzi / fenmu )
      (user,(item,predict) )
    })

    CommonUtil.loadTestData(sc, test_path).map(t2 => (t2, 2.5)).leftOuterJoin(rdd_sum.map(t2 => ((t2._1, t2._2._1), t2._2._2))).map(t2 => {
      var res = 3.4952
      if(t2._2._2 != None){
        res = t2._2._2.get
      }
      res
    }).repartition(1).saveAsTextFile(predict_path)

    rdd_sum
  }

}
