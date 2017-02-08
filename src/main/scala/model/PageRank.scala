package model

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by arachis on 2017/2/8.
 * PageRank算法：
 *  理论方面：数学之美
 *  代码参考的<<spark大数据处理>>
 */
object PageRank extends Serializable{
  def main(args: Array[String]) {
     val sc = new SparkContext(new SparkConf())
    //定义迭代次数
     val ITERATIONS = 100
    //表示一个网站的图模型，其中Tuple2的第一个元素是网页,第二个元素是该网页中的所有超链接
    val links = sc.parallelize( Array( ('A',Array('D')), ('B',Array('A')) , ('C',Array('A','B')) , ('D',Array('A','C')) ) )
    //表示每个网页的排名权重，初值权重是1
    var ranks = sc.parallelize( Array( ('A',1.0) ,('B',1.0) , ('C',1.0) , ('D',1.0) ) )

    for(i <- 0 until ITERATIONS ){
      //对于每一个URL,更新其相邻网页的排名权重rank/links.size
      val contribs = links.join(ranks).flatMap {
        case (url, (links, rank)) =>
          links.map(dest => (dest, rank/links.size))//归一化
      }
      ranks = contribs.reduceByKey((v1,v2) => v1+v2)
      ranks.collect.foreach(println)
    }


  }

}
