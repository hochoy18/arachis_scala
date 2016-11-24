package person.tzg.scala

import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by YYT on 2016/11/10.
 */
object DataSkewSolution extends Serializable{
  import org.apache.spark.rdd.RDD
  val headers = "/path/to/prefix"
  val day = "2016-10-08"
  val USER_OPERATION_R = 9
  val USER_OPERATION_OPERATION_TYPE = 4
  val USER_OPERATION_UNIQUE_ID = 1
  val USER_OPERATION_IP = 2
  val USER_OPERATION_VIDEO_ID = 6
  val USER_OPERATION_USER_ID = 3
  val USER_OPERATION_PLATFORMTYPE = 10
  import java.util.Random
  val sc = new SparkContext(new SparkConf())
  val log = sc.textFile(headers+"/"+day)
  val pairs = log.map(l => {
    (l.split(",")(USER_OPERATION_OPERATION_TYPE), 1)
  })


  /**
   * all key add random prefix
   * @return
   */
  def solution1(): RDD[(String,Int)] ={
    val SPLIT = "-"
    val prefix = new Random().nextInt(10)
    pairs.map( t2 => ( prefix+SPLIT+t2._1 ,1) ).reduceByKey((v1,v2) =>v1+v2 ).map(t2 =>(t2._1.split(SPLIT)(1) , t2._2) ).reduceByKey((v1,v2) =>v1+v2 )
  }

  val vidStatsPath = "/tmp/res/2016-10-08"
  val vid_pairs = log.map(l =>{
    (l.split(",")(USER_OPERATION_VIDEO_ID),1)
  })

  /**
   * filter and map to realize join
   * @return
   */
  def solution2():RDD[String] = {
    val broadcast = sc.broadcast( sc.textFile(vidStatsPath).map(s => {
      val SPLIT = ","
      val ss = s.split(SPLIT)
      ( ss(1) , ss(2) )
    }).collectAsMap() )
    //inner join
    vid_pairs.filter(t2 =>{
      broadcast.value.keySet.contains(t2._1)
    }).map(t2 => {
      broadcast.value.get(t2._1).get
    })

  }

  val pairs1 = sc.textFile(headers+"/"+"2016-11-08",250).map(l => {
    (l.split(",")(USER_OPERATION_OPERATION_TYPE), 1)
  })
  val pairs2 = sc.textFile(vidStatsPath).map(s => {
    val SPLIT = ","
    val ss = s.split(SPLIT)
    ( ss(1) , ss(2) )
  })

  /**
   * split some RDD with skewed key to join
   * @return
   */
  def solution3():RDD[(String,(Int,String))] = {
    val skewedKey = pairs1.sample(false,0.1).map(t2 => (t2._1 , 1) ).reduceByKey((v1,v2) =>v1+v2).map(t2 => t2.swap).sortByKey(false).first._2
    val skewedRDD = pairs1.filter(_._1.equals(skewedKey))
    val commonRDD = pairs1.filter(!_._1.equals(skewedKey) )
    val SPLIT = "_"
    val size = 100
    val skewedRDD2 = pairs2.filter(_._1.equals(skewedKey)).flatMap(t2 => {
      //multi 100
      val array = new Array[Tuple2[String, String]](size)
      for (i <- 0 to size) {
        array(i) = (new Tuple2[String, String](i + SPLIT + t2._1, t2._2))
      }
      array
    })
    val joinedRDD1 = skewedRDD.map(t2 => {
      val prefix = new Random().nextInt(size)
      (prefix + SPLIT + t2._1, t2._2)
    }).join(skewedRDD2).map(t2 => {
      val key = t2._1.split(SPLIT)(1)
      (key, t2._2)
    })

    val joinedRDD2 = commonRDD.join(pairs2)
    joinedRDD1.union(joinedRDD2)
  }

  /**
   *  many RDD with skewed key to join
   * @return
   */
  def solution4():RDD[(String,(String,String))] = {

    val SPLIT = "_"
    val size = 100
    val skewedRDD = pairs1.flatMap(t2 => {
      //multi 100
      val array = new Array[Tuple2[String, String]](size)
      for (i <- 0 to size) {
        array(i) = (new Tuple2[String, String](i + SPLIT + t2._1, t2._2.toString))
      }
      array
    })

    pairs2.map(t2 => {
      val prefix = new Random().nextInt(size)
      (prefix + SPLIT + t2._1, t2._2) //key random hash
    }).join(skewedRDD).map(t2 => {
      val key = t2._1.split(SPLIT)(1)
      (key, t2._2)
    })

  }


}
