package person.tzg.scala


import scala.collection.mutable.{ArrayBuffer}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import utils.VaildLogUtils
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
   * 加载训练集
   * @param sc
   */
  def loadTrainData(sc: SparkContext,path:String): RDD[(String, String, Double)] = {
    //parse to user,item,rating 3tuple
    val ratings = sc.textFile(path, 200).filter(l => {
      val fileds = l.split(split)
      fileds.length >=3 && VaildLogUtils.isNumeric(fileds(0)) && VaildLogUtils.isNumeric(fileds(1)) && VaildLogUtils.isNumeric(fileds(2))
    })
      .map(line => {
        val fileds = line.split(split)

        (fileds(0), fileds(1), fileds(2).toDouble)
      })
    ratings
  }

  /**
   * 加载测试集
   * @param sc
   */
  def loadTestData(sc: SparkContext,path:String): RDD[(String, String)] = {
    //parse to user,item 3tuple
    val ratings = sc.textFile(path, 200).filter(l => {
      val fileds = l.split(split)
      fileds.length >=2 && VaildLogUtils.isNumeric(fileds(0)) && VaildLogUtils.isNumeric(fileds(1))
    })
      .map(line => {
        val fileds = line.split(split)

        (fileds(0), fileds(1))
      })
    ratings
  }

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
  def similarity(sim_type: String = "cosine_s",ratings: RDD[(String, String, Double)]): RDD[(String, String, Double)] = {

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
      (item, n)//item,count
    })

    val rdd_left: RDD[(String, (String, String, Int,Int))] = common_item_matrix.map(t2 => {
      (t2._1._1, (t2._1._1, t2._1._2, t2._2)) //item1,item2,count
    }).join(rdd_right).map(t2 =>{ //item1,item2,f12,f2
      (t2._2._1._2,(t2._2._1._1,t2._2._1._2,t2._2._1._3,t2._2._2))
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
    //矩阵计算——用户：用户对结果排序，过滤
    item_predict.groupByKey.map(f => {
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
    val ratings: RDD[(String, String, Double)] = loadTrainData(sc,train_path)

    //calculate similarity
    val rdd_cosine_s: RDD[(String, String, Double)] = similarity("cosine_s",ratings) //(8102,6688,0.006008038124054778)

    //recommend to user top n
    val recommend_ = recommend(rdd_cosine_s, ratings,10)

    //predict item rating
    val predict_ = predict(rdd_cosine_s, ratings, 10)

    //predict item rating
    // 冷启动，如果用户没有评分，默认是平均值
    loadTestData(sc, test_path).map(t2 => (t2, 2.5)).leftOuterJoin(predict_.map(t2 => ((t2._1, t2._2._1), t2._2._2))).map(t2 => {
      var res = 3.4952
      if(t2._2._2 != None){
        res = t2._2._2.get
      }
      res
    }).repartition(1).saveAsTextFile(predict_path)
  }

}
