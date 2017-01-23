package person.tzg.scala.test

import org.apache.spark.rdd.{CoGroupedRDD, RDD}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * Created by arachis on 2016/12/26.
 *
 * 测试Spark RDD join 的分区和依赖关系：
 * getPartition , partitions
 * dependency
 *
 */
object MyJoinTest extends Serializable {

  val sc = new SparkContext(new SparkConf())

  def main(args: Array[String]) {
    val video: RDD[String] = sc.textFile("data/1.csv")
    val area: RDD[String] = sc.textFile("data/2.csv")

    val SPLIT = ","
    val VideoPair = video.map(s => {
      val ss = s.split(SPLIT)
      (ss(0), ss(1))
    })

    VideoPair.partitions
    //这里两个RDD 相等，就不验证了
    //res28: Int = 2

    val AreaPair = area.map(s => {
      val ss = s.split(SPLIT)
      (ss(0), ss(1))
    })
    AreaPair.partitioner
    //键值对类型才有Partitioner
    //Option[org.apache.spark.Partitioner] = None


    //阅读源码之后，知道底层使用rdd1.join(rdd2,defaultPartitioner(,))
    val joinRDD1: RDD[(String, (String, String))] = VideoPair.map {
      t2 => t2.swap
    }.join(AreaPair)
    //因为这里没有指定任何分区函数，所有返回的分区函数时HashPartitioner
    joinRDD1.partitioner
    //res26: Option[org.apache.spark.Partitioner] = Some(org.apache.spark.HashPartitioner@2)

    joinRDD1.dependencies
    //Seq[org.apache.spark.Dependency[_]] = List(org.apache.spark.OneToOneDependency@45421fc5)

    //确定了分区函数后，继续走到join的源码实现：rdd1.join(rdd2) => rdd1.cogroup(rdd2,partitioner)
    val cogroupRDD2: RDD[(String, (Iterable[String], Iterable[String]))] = VideoPair.map {
      t2 => t2.swap
    }.cogroup(AreaPair, new HashPartitioner(2))

    cogroupRDD2.partitioner
    //查看一些分区函数，因为后面的rdd的dependencies和其有关
    //res29: Option[org.apache.spark.Partitioner] = Some(org.apache.spark.HashPartitioner@2)


    cogroupRDD2.dependencies
    //res40: Seq[org.apache.spark.Dependency[_]] = List(org.apache.spark.OneToOneDependency@365ad868)

    //返回的join结果就是分别遍历然后返回的tuple
    cogroupRDD2.flatMapValues(pair => for (v <- pair._1.iterator; w <- pair._2.iterator) yield (v, w))
    //res30: org.apache.spark.rdd.RDD[(String, (String, String))] = MapPartitionsRDD[36] at flatMapValues at <console>:47
    //这里yield 关键字不太熟悉，google了下，可以查看这个链接：http://www.liaoxuefeng.com/article/001373892916170b88313a39f294309970ad53fc6851243000

    //更近一步，跟到cogroup这个方法看下到底发生了什么
    val cg = new CoGroupedRDD[String](Seq(VideoPair.map {
      t2 => t2.swap
    }, AreaPair), new HashPartitioner(2))
    //终于走到了最后，看到创建了一个CoGroupedRDD

    //看下这个rdd的依赖,这里是CoGroupedRDD的getDependencies的具体实现了，因为VideoPair和AreaPair的分区函数都是None，所以得到依赖是宽依赖
    cg.dependencies
    //res37: Seq[org.apache.spark.Dependency[_]] = List(org.apache.spark.ShuffleDependency@6ef67875, org.apache.spark.ShuffleDependency@775e823c)


    /**
     * 所以分析到这，我得到了一下结论：
     * 1.join内部实现 ： rdd1.join(rdd2) => rdd1.join(rdd2,defaultPartitioner(rdd1,rdd2)) => rdd1.cogroup(rdd2,defaultPartitioner(rdd1,rdd2)) => new CoGroupedRDD(Seq(rdd1,rdd2),defaultPartitioner(rdd1,rdd2)).mapValues().flatMapValues()
     * 2.cogroup 算子的依赖关系是宽依赖还是窄依赖取决于和目标分区函数是否相同，如果相同则为窄依赖；否则为宽依赖
     *
     */


  }


}
