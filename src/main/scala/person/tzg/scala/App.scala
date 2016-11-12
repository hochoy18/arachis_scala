package person.tzg.scala


import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
/**
 * Created by arachis on 11/11/16.
 */
object Exe1 extends Serializable{
  def main(args:Array[String]): Unit= {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Exe1")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("data.txt")
    val pairs = lines.map(s => (s, 1))
    val counts = pairs.reduceByKey((a, b) => a + b)

    sc.parallelize(Seq(("A", 1), ("B", 2), ("C", 3), ("A", 4), ("B", 5)))
      .map(t2 => (t2._1,t2._2) ).sortByKey()

  }
}
