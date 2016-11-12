package person.tzg.scala

/**
 * Created by hadoop on 11/11/16.
 */
object Exe1 extends Serializable{
  def main(args:Array[String]): Unit= {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Exe1").set("spark.executor.memory")
    val sc = new SparkContext(conf)
    val kv1 = sc.parallelize(List(("A", 1), ("B", 2), ("C", 3), ("A", 4), ("B", 5)))
    kv1.sortByKey().collect
  }
}
