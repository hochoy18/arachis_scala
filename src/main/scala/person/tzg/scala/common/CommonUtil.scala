package person.tzg.scala.common

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import utils.VaildLogUtils

/**
 * Created by arachis on 2016/11/24.
 */
object CommonUtil extends Serializable{

  val train_path: String = "/tmp/train.csv"
  val test_path = "/tmp/test.csv"
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
   * 计算物品评分均值
   * @param user_rating 原始评分矩阵
   * @return （item,mean）
   */
  def getMean(user_rating: RDD[(String, String, Double)]): RDD[(String,Double)] ={

    val rdd_item_mean = user_rating.map(t3 => (t3._2,(t3._3,1))).reduceByKey((v1,v2)=>( v1._1+v2._1,v1._2+v2._2 )).map(t2 =>{
      val item = t2._1
      val mean = ( t2._2._1 / t2._2._2 )
      (item,1.0 * (mean * 10000).toInt / 10000)
    })
    rdd_item_mean
  }

}
