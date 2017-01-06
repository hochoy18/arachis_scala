package person.tzg.scala

import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import person.tzg.scala.common.CommonUtil

/**
 * Created by arachis on 2016/11/24.

 * http://spark.apache.org/docs/2.0.0/mllib-data-types.html#coordinatematrix
 *
 */
object demo extends Serializable {

  val conf = new SparkConf().setAppName(this.getClass.getName)
  val sc = new SparkContext(conf)
  val HDFS = "hdfs://192.168.1.222:8020"
  val train_path: String = HDFS + "/tmp/train.csv"
  val test_path = HDFS + "/tmp/test.csv"
  val predict_path = HDFS + "/user/tongzhenguo/recommend/cosine_predict"
  val split: String = ","

  def main(args: Array[String]) {

    //load user rating data to BlockMatrix
    val ratingData = CommonUtil.loadTrainData(sc, train_path)
    val user_rating_entrys = ratingData.map { case (userID, itemID, rating) =>
      new MatrixEntry(userID.toLong, itemID.toLong, rating.toDouble)
    }
    val ratingMat: CoordinateMatrix = new CoordinateMatrix(user_rating_entrys)
    val item_rating = ratingMat.toIndexedRowMatrix().toBlockMatrix().transpose
    //item_rating.validate

    val multiply: BlockMatrix = item_rating.multiply(item_rating)
    val item_squr: RDD[((Int, Int), Matrix)] = multiply.blocks.filter {
      case ((item1, item2), sim) => item1 == item2
    }
    val item12: RDD[((Int, Int), Matrix)] = multiply.blocks.filter {
      case ((item1, item2), sim) => item1 != item2
    }
    //item12.join()



    val item_sim_entry = sc.textFile(HDFS + "/user/tongzhenguo/recommend/sim_cosine").map(s => {
      val ss = s.replaceAll("\\(", "").replaceAll("\\)", "").split(",")
      (ss(0).toLong, ss(1).toLong, ss(2).toDouble)
    }).distinct.map { case (userID, itemID, sim) =>
      new MatrixEntry(userID, itemID, sim)
    }
    //load sim pair to CoordinateMatrix
    val simMat: CoordinateMatrix = new CoordinateMatrix(item_sim_entry)
    //A CoordinateMatrix can be converted to an IndexedRowMatrix with sparse rows by calling toIndexedRowMatrix.
    // Other computations for CoordinateMatrix are not currently supported.
    val simMatrix = simMat.toIndexedRowMatrix().toBlockMatrix().cache()
    simMatrix.validate

    val mm8 = simMatrix.multiply(item_rating)

    val mm9 = mm8.transpose.toIndexedRowMatrix()

    val mm10 = mm9.rows.map {
      case row =>
        val uid = row.index
        val item_pref_vector = row.vector
        (uid, item_pref_vector)
    }.sortByKey()

  }


  def predict(item_similarity: RDD[(String, String, Double)], user_rating: RDD[(String, String, Double)], sc: SparkContext): RDD[(String, (String, Double))] = {
    val numPartitions: Int = 500

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

    val rdd_item_mean = CommonUtil.getMean(user_rating)
    val item_mean_map = rdd_item_mean.collectAsMap()
    val broadcast = sc.broadcast(item_mean_map)

    val rdd_item_diff = user_rating.map(t3 => (t3._2, (t3._1, t3._3))).map(t2 => {
      val item = t2._1
      val user = t2._2._1
      val r: Double = t2._2._2
      var mean = 3.4952
      if (None != broadcast.value.get(item)) {
        mean = broadcast.value.get(item).get
      }
      val diff = r - mean
      (item, (user, diff))
    })

    val rdd_1 = item_similarity.map(t3 => (t3._2, (t3._1, t3._3))).join(rdd_item_diff, numPartitions).map(t2 => {

      val item = t2._2._1._1
      val user = t2._2._2._1
      val wi: Double = t2._2._1._2
      val r_diff: Double = t2._2._2._2
      val weight = wi * r_diff
      val fenzi: Double = 1.0 * (weight * 10000).toInt / 10000
      val fenmu: Double = 1.0 * (wi * 10000).toInt / 10000
      ((user, item), (fenzi, fenmu))
    })

    val rdd_sum = rdd_1.reduceByKey((v1_t2, v2_t2) => {
      val sum_fenzi: Double = v1_t2._1 + v2_t2._1
      val sum_fenmu: Double = v1_t2._2 + v2_t2._2
      (sum_fenzi, sum_fenmu)
    }, numPartitions).map(t2 => {

      val user = t2._1._1
      val item = t2._1._2
      var mean = 3.4952
      if (None != broadcast.value.get(item)) {
        mean = broadcast.value.get(item).get
      }
      val fenzi: Double = t2._2._1
      val fenmu: Double = t2._2._2
      val predict = mean + (fenzi / fenmu)
      (user, (item, predict))
    })

    CommonUtil.loadTestData(sc, test_path).map(t2 => (t2, 2.5)).leftOuterJoin(rdd_sum.map(t2 => ((t2._1, t2._2._1), t2._2._2)), numPartitions).map(t2 => {
      var res = 3.4952
      if (t2._2._2 != None) {
        res = t2._2._2.get
      }
      res
    }).repartition(1).saveAsTextFile(predict_path)

    rdd_sum
  }

}
