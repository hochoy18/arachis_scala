package person.tzg.scala.feature_engineering

import java.util.Date

import hdfs.HDFSUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import utils.DateUtils

/**
 * Created by arachis on 2017/1/11.
 * 找到KL散度高的搜索词
 * 参考文章链接：http://www.cnblogs.com/charlotte77/p/5392052.html
 *
 */
object UserSearchDataSet extends Serializable{

  val sc = new SparkContext(new SparkConf())
  val USER_SEARCH_PRE = "/my/path/search"
  val search_cpre_path: String = "/user/tongzhenguo/data/search/search_c_"
  val SPLIT: String = ","

  def combineLog(start:Date,end:Date): RDD[String] ={
    val startDate: String = DateUtils.toDateString(start)
    val endDate: String = DateUtils.toDateString(end)
    val startDay: String = startDate.replaceAll("-","")
    val endDay: String = endDate.replaceAll("-","")

    var container = sc.textFile(USER_SEARCH_PRE+DateUtils.toDateString(start),12)

    var tmp = DateUtils.plusDays(start,1)
    while( tmp.getTime <= end.getTime ){
      val date: String = DateUtils.toDateString(tmp)
      container = container.union(sc.textFile(USER_SEARCH_PRE+date,12))
      tmp = DateUtils.plusDays(tmp,1)
    }
    container
  }

  /**
    *  从搜索日志中提取出目标群体（女，25-29）和对比样本
    */
  def sample(base: RDD[(String, String)]):RDD[((String, Int), String)]={

    val user_age = "/user/tongzhenguo/data/user_age/"
    val age: RDD[String] = sc.textFile(user_age)
    val teenUid = age.filter(_.split(SPLIT)(1).equals("3")).map(_.split(SPLIT)(0))
    val notTeenUid = age.filter(! _.split(SPLIT)(1).equals("3")).map(_.split(SPLIT)(0))

    val gender = sc.textFile("/user/taoyizhou/features/user/dataset_sex_is_not_-1_id").map(s => {
      val ss = s.split(SPLIT)
      (ss(1), ss(0))
    })

    val female: RDD[(String, String)] = gender.filter(_._2.equals("0"))
    val male: RDD[(String, String)] = gender.filter(_._2.equals("1"))

    val targetUsers = teenUid.map(uid=>(uid,1)).join(female).map(t2=>(t2._1,1))
    val notTargetUsers = notTeenUid.map(uid=>(uid,1)).join(male).map(t2=>(t2._1,1))

    val pos = targetUsers.join(base).map{case (uid,(one,word)) => ((word,1),uid)}
    val neg = notTargetUsers.join(base).map{case (uid,(one,word)) => ((word,0),uid)}

    val sampleRDD = neg.sample(false,1.0*pos.count /neg.count)
    
    pos.union(sampleRDD)
  }

  /**
   * 计算KL散度
   * @param p:真实分布
   * @param q:理想分布
   */
  def kl(p: Array[Double],q: Array[Double]): Double ={
    val n = p.length
    var sum = 0.0
    for(i <- 0 until n){
      if(p(i)/q(i) >0){
        sum += p(i) * math.log(p(i)/q(i))
      }else{ //异常处理
        sum += 1
      }
    }
    sum
  }

  /**
   * 找到区分目标用户和对比用户群体的词，返回KL 散度和搜索词
   */
  def getImportSearchC(c_stats: RDD[(String, (Int, Int, Int))]): RDD[(Double, String)] ={

    val Dkl_Search_C: RDD[(Double, String)] = c_stats.groupByKey().map {
      case t2 => {
        val p = Array(0.0, 0.0) ;val q = Array(0.0, 0.0)
        var sum1 = 0.0 ;var sum2 = 0.0
        val iterator: Iterator[(Int, Int, Int)] = t2._2.iterator
        while (iterator.hasNext) {
          val t3: (Int, Int, Int) = iterator.next()
          p(t3._1) = t3._2; sum1 += p(t3._1)
          q(t3._1) = t3._3; sum2 += q(t3._1)
        }
        if (sum1 != 0 && sum2 != 0) {
          //防止除0异常
          p(0) /= sum1;p(1) /= sum1;
          q(0) /= sum2;q(1) /= sum2;
          (t2._1, kl(p, q))
        } else {
          (t2._1, Double.MaxValue)
        }
      }
    }.map(_.swap).sortByKey(false).filter(_._1 >= 0.5)

    Dkl_Search_C
  }

  def main(args: Array[String]) {

    val SEARCH_USER_ID = 3
    val SEARCH_C = 13

    var yesterday = DateUtils.plusDays(new Date(),-1)
    if(args.length>=1){
      yesterday = DateUtils.plusDays(new Date(),-args(0).toInt)
    }

    val log: RDD[String] = combineLog(DateUtils.plusDays(yesterday,-30),yesterday).repartition(132)

    val base = log.filter(s => {
      val ss: Array[String] = s.split(SPLIT)
      ss.length >= 16 && ! "0".equals(ss(SEARCH_USER_ID))
    }).map(s => {
      val ss: Array[String] = s.split(SPLIT)
      val uid = ss(SEARCH_USER_ID)
      val c = ss(SEARCH_C)
      ( uid, c)
    })
    base.count()

    val sampleUsers = sample(base).repartition(132)

    val c_pv = sampleUsers.map{ case ((word,label),uid) => ((word,label),1) }.reduceByKey((v1,v2) => v1+v2)
    val c_uv = sampleUsers.distinct.map{ case ((word,label),uid) => ((word,label),1) }.reduceByKey((v1,v2) => v1+v2)
    val c_stats = c_pv.join(c_uv).map{ case ((c, label), (freq, count)) => (c, (label, freq, count))  }

    val search_c:RDD[(Double, String)] = getImportSearchC(c_stats)
    HDFSUtils.rmrIfExist(search_cpre_path+DateUtils.toDString(yesterday))
    search_c.map(_._2).repartition(1).saveAsTextFile(search_cpre_path+DateUtils.toDString(yesterday))

    val withIndex: RDD[(String, Long)] = search_c.repartition(132).map(_._2).zipWithIndex().map{case (str,idx)=>(str,idx+1)}

    HDFSUtils.rmrIfExist("/user/tongzhenguo/data/search/userSearchDataSet"+DateUtils.toDString(yesterday))
    withIndex.join(sampleUsers.map{case ((word,label),uid) => (word,(label,uid))}).map{
      case (word,(idx,((label,uid)))) => ( (label,uid,idx),1)
    }.reduceByKey((v1,v2) => v1+v2).map{
      case ( (label,uid,idx),freq) => ((label,uid.toLong),(idx,freq))
    }.groupByKey().map{
      case ( (label,uid),values ) =>{
        val iter = values.iterator
        var str = ""
        val list: List[(Long, Int)] = List.toList(iter).sorted
        for(t2 <- list){
          str += t2._1 +":"+t2._2+" "
        }
        label +" "+str.substring(0,str.length-1)
      }
    }.saveAsTextFile("/user/tongzhenguo/data/search/userSearchDataSet"+DateUtils.toDString(yesterday))
  }

}
