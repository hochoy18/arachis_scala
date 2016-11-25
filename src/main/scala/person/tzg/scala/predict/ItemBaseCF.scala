package person.tzg.scala.predict

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import person.tzg.scala.common.CommonUtil

/**
 * Created by arachis on 2016/11/24.
 * 基于物品的协同过滤的评分预测算法
 * 物品相似度矩阵过于庞大：如果物品有5w,那存储相似矩阵就要5w * 5w
 * 此矩阵的数据结构一定要优化：
 *  将用户评分数据和物品相似度数据以spark mllib中的Matrix形式存储
 *  计算评分
 *  参考自：
 *    http://3iter.com/2015/10/12/用spark实现基于物品属性相似度的推荐算法/
 *    http://spark.apache.org/docs/2.0.0/mllib-data-types.html#coordinatematrix
 *
 */
object ItemBaseCF extends Serializable{

  val conf = new SparkConf().setAppName(this.getClass.getName)
  val sc = new SparkContext(conf)
  val HDFS = "hdfs://192.168.1.222:8020"
  val train_path: String = HDFS+"/tmp/train.csv"
  val test_path = HDFS+"/tmp/test.csv"
  val predict_path = HDFS+"/user/tongzhenguo/recommend/cosine_predict"
  val split: String = ","

  def main(args: Array[String]) {

    val item_sim_entry = sc.textFile(HDFS+"/user/tongzhenguo/recommend/sim_cosine").map(s => {
      val ss = s.replaceAll("\\(", "").replaceAll("\\)", "").split(",")
      ( ss(0).toLong, ss(1).toLong, ss(2).toDouble )
    }).distinct.map{case (userID,itemID,sim) =>
      new MatrixEntry(userID,itemID,sim)
    }
    //load sim pair to CoordinateMatrix
    val simMat: CoordinateMatrix = new CoordinateMatrix(item_sim_entry)
    //A CoordinateMatrix can be converted to an IndexedRowMatrix with sparse rows by calling toIndexedRowMatrix.
    // Other computations for CoordinateMatrix are not currently supported.
    val simMatrix = simMat.toIndexedRowMatrix().toBlockMatrix().cache()
    simMatrix.validate

    //load user rating data to BlockMatrix
    val ratingData = CommonUtil.loadTrainData(sc, train_path)
    val user_rating_entrys = ratingData.map { case (userID, itemID, rating) =>
      new MatrixEntry(userID.toLong, itemID.toLong, rating.toDouble)
    }
    val ratingMat: CoordinateMatrix = new CoordinateMatrix(user_rating_entrys)
    val item_rating = ratingMat.toIndexedRowMatrix().toBlockMatrix().transpose
    item_rating.validate

    val mm8 = simMatrix.multiply(item_rating)
    //矩阵转置
    val mm9 = mm8.transpose.toIndexedRowMatrix()

    val mm10 = mm9.rows.map{
      case row =>
        val uid = row.index
        val item_pref_vector = row.vector
        (uid,item_pref_vector)
    }.sortByKey()

  }


  /**
   * 预测用户对物品的评分
   * @param item_similarity 物品相似度  151098234 pairs
   * @param user_rating 用户评分数据
   * @return (user,(item_j,predict) )
   */
  def predict(item_similarity: RDD[(String, String, Double)], user_rating: RDD[(String, String, Double)],sc:SparkContext): RDD[(String, (String, Double))] = {
    val numPartitions: Int = 500

    //缩小相似度矩阵，只留下相似度最高的前100个
    val sorted_item_sim: RDD[(String, String, Double)] = item_similarity.map(t3 => {
      (t3._1, (t3._2, t3._3))
    }).groupByKey().map(f => {
      val i2 = f._2.toBuffer
      val i2_2 = i2.sortBy(_._2)
      if (i2_2.length > 100) {
        i2_2.remove(0, (i2_2.length - 100))
      }
      (f._1, i2_2.toIterable)
    }).flatMap(f => {
      val id2 = f._2
      for (w <- id2) yield (f._1, w._1, w._2)

    })

    //计算物品评分均值
    val rdd_item_mean = CommonUtil.getMean(user_rating)
    val item_mean_map = rdd_item_mean.collectAsMap()
    val broadcast = sc.broadcast(item_mean_map)
    
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
    val rdd_1 = item_similarity.map(t3 => (t3._2, (t3._1, t3._3))).join(rdd_item_diff,numPartitions).map(t2 => {

      val item = t2._2._1._1
      val user = t2._2._2._1
      val wi: Double = t2._2._1._2
      val r_diff: Double = t2._2._2._2
      val weight = wi * r_diff
      val fenzi: Double = 1.0 * (weight * 10000).toInt / 10000
      val fenmu: Double = 1.0 * (wi * 10000).toInt / 10000
      ( (user, item), (fenzi,fenmu) )
    })

    //矩阵计算——用户：元素累加求和
    val rdd_sum = rdd_1.reduceByKey((v1_t2,v2_t2)=>{
      val sum_fenzi: Double = v1_t2._1+v2_t2._1
      val sum_fenmu: Double = v1_t2._2+v2_t2._2
      (sum_fenzi,sum_fenmu)
    },numPartitions).map(t2 => {

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

    CommonUtil.loadTestData(sc, test_path).map(t2 => (t2, 2.5)).leftOuterJoin(rdd_sum.map(t2 => ((t2._1, t2._2._1), t2._2._2)),numPartitions).map(t2 => {
      var res = 3.4952
      if(t2._2._2 != None){
        res = t2._2._2.get
      }
      res
    }).repartition(1).saveAsTextFile(predict_path)

    rdd_sum
  }

}
