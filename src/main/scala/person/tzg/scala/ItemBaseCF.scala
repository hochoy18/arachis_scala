package person.tzg.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
 * Created by arachis on 2016/11/21.
 * spark 实现基于物品的协同过滤
 * 参考：http://blog.csdn.net/sunbow0/article/details/42737541
 */
object ItemBaseCF extends Serializable {

  val train_path: String = "/tmp/train.csv"
  val test_path = "/tmp/test.csv"
  val split: String = ","

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)

    //parse to user,item,rating 3tuple
    val ratings: RDD[(String, String, Double)] = loadDate(sc)

    //convert to user-item matrix
    val user_item: RDD[(String, String)] = ratings.map(t3 => (t3._1, t3._2)).sortByKey().cache()

    //calculate the cooccurence matrix of item
    val cooccurence_matrix: RDD[((String, String), Int)] = calculateCoccurenceMatrix(user_item)

    val same_item_matrix: RDD[((String, String), Int)] = cooccurence_matrix.filter(t2 => {
      t2._1._1 == t2._1._1
    })

    val common_item_matrix: RDD[((String, String), Int)] = cooccurence_matrix.filter(t2 => {
      t2._1._1 != t2._1._1
    })

    //calculate the similarity of between item1 and item2
    val rdd_right: RDD[(String, Int)] = same_item_matrix.map(t2 => {

      val item_i: String = t2._1._1
      val ni: Int = t2._2
      (item_i, ni)
    })

    val rdd_left: RDD[(String, (String, String, Int, Int))] = common_item_matrix.map(t2 => {
      (t2._1._1, (t2._1._1, t2._1._2, t2._2)) //item1,item2,count
    }).join(rdd_right).map(t2 => {
      val item1 = t2._2._1._1
      val item2 = t2._2._1._2
      val nij = t2._2._1._3
      val ni = t2._2._2
      (item2, (item1, item2, nij, ni))
    })

    val rdd_cosine_s: RDD[(String, String, Double)] = similarity("cosine_s", rdd_left, rdd_right)

  }

  /**
   * 加载数据为用户评分矩阵
   * @param sc
   */
  def loadDate(sc: SparkContext): RDD[(String, String, Double)] = {
    //parse to user,item,rating 3tuple
    val ratings = sc.textFile(train_path, 100).map(line => {
      val fileds = line.split(split)

      (fileds(0), fileds(1), fileds(2).toDouble)
    })
    ratings
  }

  /**
   * 计算物品共现矩阵
   * @param user_item 用户-物品评分矩阵
   * @return
   */
  def calculateCoccurenceMatrix(user_item: RDD[(String, String)]): RDD[((String, String), Int)] = {
    val cooccurence_matrix: RDD[((String, String), Int)] = user_item.join(user_item).map(t2 => {
      (t2._2, 1) //item,item : 1
    }).reduceByKey((v1, v2) => v1 + v2)
    cooccurence_matrix
  }

  /**
   * 计算物品的相似度算法
   * @param sim_type 相似度计算方法
   * @param rdd_left
   * @param rdd_right
   * @return 物品i和物品j的相似度
   */
  def similarity(sim_type: String = "cosine_s", rdd_left: RDD[(String, (String, String, Int, Int))], rdd_right: RDD[(String, Int)]): RDD[(String, String, Double)] = {
    return rdd_left.join(rdd_right).map(t2 => {
      val item_i = t2._2._1._1
      val item_j = t2._2._1._2
      val nij = t2._2._1._3
      val ni = t2._2._1._4
      val nj = t2._2._2

      (item_i, item_j, (nij / math.sqrt(ni * nj))) //cosine_s
    })
  }

}
