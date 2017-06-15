/*
package person.tzg.scala

import java.lang

import org.apache.spark.streaming.dstream.DStream


/**
 * Created by Tongzhenguo on 2017/6/12.
 * 实现腾讯的real time itembased 论文中的实时剪枝
 */
object Real_time_Pruning {

  def main(args: Array[String]) {

    var dStream:DStream[(String, (Int, Int))] = null//包含item_count或者pair_count的结构:(iid,rat,cnt)
    dStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          iter => {

            val r = new Jedis(REDIS_HOST,REDIS_PORT,EXPIRE_TIME)
            val pipe  = r.pipelined()

            for ( (key,(rat,nij)) <- iter ) {

              val MAX_COUNT: Long = 30L //一个paircount或者itemcount最多的保存数量
              if (key.contains("_")) {

                val pairij: Array[String] = key.split("_")
                val (iidi, iidj) = (pairij(0), pairij(1))
                var itemi_count = 0.0
                val iterator_itemi: java.util.Iterator[Tuple] = r.zrangeWithScores(ITEMCOUNT_PAIRCOUNT_KEY_PREFIX + "itemcount." + iidi,0,-1).iterator() //
                while (iterator_itemi.hasNext) {
                  val score: Double = iterator_itemi.next().getScore
                  itemi_count += score
                }

                var itemj_count = 0.0
                val iterator_itemj: java.util.Iterator[Tuple] = r.zrangeWithScores(ITEMCOUNT_PAIRCOUNT_KEY_PREFIX + "itemcount." + iidj,0,-1).iterator() //
                while (iterator_itemj.hasNext) {
                  val score: Double = iterator_itemj.next().getScore
                  itemj_count += score
                }

                val PAIR_ITEMI_KEY = ITEMCOUNT_PAIRCOUNT_KEY_PREFIX + "paircount." + iidi
                val PAIR_ITEMJ_KEY: String = ITEMCOUNT_PAIRCOUNT_KEY_PREFIX + "paircount." + iidj
                val RECLIST_ITEMI_KEY: String = ITEMBASED_RECLIST_KEY_PREFIX + iidi
                val RECLIST_ITEMJ_KEY: String = ITEMBASED_RECLIST_KEY_PREFIX + iidj

                if (r.zscore(RECLIST_ITEMI_KEY, iidj) == null && itemi_count>0 && itemj_count>0) {

                  //pair-rat SortedSet Type UPDATE AND recent 30 item
                  if (r.zcard(PAIR_ITEMI_KEY) >= MAX_COUNT) {
                    pipe.zremrangeByRank(PAIR_ITEMI_KEY, 0, 0)
                    pipe.zincrby(PAIR_ITEMI_KEY, rat, iidj)
                    pipe.expire(PAIR_ITEMI_KEY, EXPIRE_TIME)
                    pipe.sync()
                  } else if (r.zcard(PAIR_ITEMJ_KEY) >= MAX_COUNT) {
                    pipe.zremrangeByRank(PAIR_ITEMJ_KEY, 0, 0)
                    pipe.zincrby(PAIR_ITEMJ_KEY, rat, iidi)
                    pipe.expire(PAIR_ITEMJ_KEY, EXPIRE_TIME)
                    pipe.sync()
                  } else {
                    pipe.zincrby(PAIR_ITEMI_KEY, rat, iidj)
                    pipe.expire(PAIR_ITEMI_KEY, EXPIRE_TIME)
                    pipe.zincrby(PAIR_ITEMJ_KEY, rat, iidi)
                    pipe.expire(PAIR_ITEMJ_KEY, EXPIRE_TIME)
                    pipe.sync()
                  }

                  //compute simij
                  val pairCount = r.zscore(PAIR_ITEMI_KEY, iidj) match {
                    case null => 0
                    case zs => zs.toInt
                  }
                  var simij = (pairCount / math.sqrt(itemi_count * itemj_count) * 1000).toInt / 1000.0
                  simij =if(simij<1){
                    simij
                  } else{
                    0
                  }

                  //compute epsilon
                  val theta = 0.95
                  val epsilon = math.sqrt( math.log(1/theta) / (2*nij) ) //math.sqrt( math.log(1/0.95) / 2 )=0.160

                  //get minsim
                  var min_simi = .0
                  var min_simj = .0
                  val iteratori: java.util.Iterator[Tuple] = r.zrangeWithScores(RECLIST_ITEMI_KEY, 0, 0).iterator() //
                  while (iteratori.hasNext) {
                    val s: Double = iteratori.next().getScore
                    if (min_simi > s) {
                      min_simi = s
                    }
                  }
                  val iteratorj: java.util.Iterator[Tuple] = r.zrangeWithScores(RECLIST_ITEMJ_KEY, 0, 0).iterator() //
                  while (iteratorj.hasNext) {
                    val s: Double = iteratorj.next().getScore
                    if (min_simj > s) {
                      min_simj = s
                    }
                  }
                  val min_sim = math.min(min_simi, min_simj)

                  //刚开始的时候推荐列表为空,min_sim=0,直接加item
                  //epsilon＜min_sim - simij时，simij以0.95的概率大于min_sim
                  if( epsilon<math.abs(min_sim - simij) || min_sim==0 ){
                    //add i to Lj
                    pipe.zadd(RECLIST_ITEMJ_KEY,simij,iidi )
                    pipe.expire(RECLIST_ITEMJ_KEY, RECLIST_EXPIRE_TIME )
                    //add j to Li
                    pipe.zadd( RECLIST_ITEMI_KEY,simij,iidj )
                    pipe.expire( RECLIST_ITEMI_KEY, RECLIST_EXPIRE_TIME )
                    pipe.sync()                                                           //
                    //大于20item，更新min_sim
                    if (r.zcard(RECLIST_ITEMI_KEY) > 20L) {
                      r.zremrangeByRank(RECLIST_ITEMI_KEY, 0, 0)                         //
                    }
                    if( r.zcard(RECLIST_ITEMJ_KEY)> 20L ){
                      r.zremrangeByRank(RECLIST_ITEMJ_KEY, 0, 0)                        //
                    }
                  }
                }
              } else {
                //item-rat SortedSet Type UPDATE
                val rkey = ITEMCOUNT_PAIRCOUNT_KEY_PREFIX + "itemcount." + key
                val cnt: lang.Long = r.zcard(rkey)

                if (cnt >= MAX_COUNT) {
                  pipe.zrem(rkey, "1") //移除最早的评分，1是插入时的时间顺序
                  pipe.zincrby(rkey, rat, MAX_COUNT.toString)
                  pipe.expire(rkey, EXPIRE_TIME)
                  pipe.sync()
                } else {
                  pipe.zincrby(rkey, rat, (cnt+1).toString)//新增一个评分只要时间加1即可，从1开始
                  pipe.expire(rkey, EXPIRE_TIME)
                  pipe.sync()
                }
              }
            }
          } )
      } )


  }



}
*/
