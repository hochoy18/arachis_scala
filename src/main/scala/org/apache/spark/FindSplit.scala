package org.apache.spark

import org.apache.spark.sql.functions.rand
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.collection.mutable.ArrayBuilder

/**
  * Created by tongzhenguo on 2018/6/13.
  * desc:改写spark决策树的分割算法为互信息相关性分析法的内置离散化方法
  * 需求分析：
  * 输入：input：DataFrame,maxBins:int,最大特征数，要求大于2,建议值32,且大于等于字符串型列独立值个数，由于抽样样本最多1w，maxBins也不超过一万
  * 输出：output：DataFrame,以上界分割点或者字符串枚举为值重新构建的一个数据框
  * 实现：
  * 测试：
  */
object FindSplit {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .getOrCreate()
    import spark.implicits._

    // 测试离散化分区方法
    var input: DataFrame = spark.sparkContext.parallelize(
      0 until 10).map(_.toString).toDF(
      "id").withColumn(
      "a", rand(seed=10)
    ).withColumn("b", rand(seed=27)
    )
    val input2: DataFrame = spark.sparkContext.parallelize(
      0 until 10).map(_.toString).toDF(
      "id").withColumn(
      "a", rand(seed=10)
    ).withColumn("b", rand(seed=27)
    )

    input = input.union(input2)

    getSplitResult(input,maxBins = 10)

  }


  def getSplitResult(input:DataFrame,maxBins:Int=32):DataFrame = {
    // todo 统计类别型独立值个数，验证maxBins是否正确
    var categoryMaxDistinCount = 0L
    var continuousFeatures = Array[String]()
    for((colname,coltype) <- input.dtypes ){

      if("StringType".equals(coltype)){
        val featValCount = input.select(colname).distinct().count()
        if(featValCount > categoryMaxDistinCount){
          categoryMaxDistinCount = featValCount
        }
      }else{
        continuousFeatures = continuousFeatures :+ colname
      }
    }

    if(maxBins < categoryMaxDistinCount){
      val formatMsg = "maxBins must more than category column maximum distinct value count,%s<%s"
        .format(maxBins, categoryMaxDistinCount)
      throw new IllegalArgumentException(formatMsg )
    }
    if(maxBins > 100000){
      val formatMsg = "maxBins must less than sample example count,%s>%s".format(maxBins, 10000)
      throw new IllegalArgumentException(formatMsg)
    }

    // todo 改造findSplits,并调用之，根据结果构造output
    val schema = input.schema
    val columns = input.columns
    val numExamples = input.count()
    var splits:Array[Array[_ >: Double with String]] = findSplits(maxBins,numExamples,continuousFeatures,input,seed = 888)

    println("show splits:")
    for((fields,idx) <- splits.zipWithIndex){
      println("%s:%s".format(input.columns(idx),fields.mkString(",")))
    }

    // todo 以分割点替换特征值
    val retRDD = input.rdd.map(row => {
      var newRow = Array[Any]()
      for(idx <- 0 until row.length){
        val newVal = if(!continuousFeatures.contains(columns(idx))){
          row(idx).toString
        }else{
          // todo 二分查找分割点
          find(row(idx).toString.toDouble,splits(idx).map(_.toString.toDouble))
        }
        newRow = newRow :+ newVal
      }
      Row(newRow:_*)
    })
    val retDF = input.sparkSession.createDataFrame(retRDD,input.schema)
    retDF.show()
    /*
+---+-------------------+-------------------+
| id|                  a|                  b|
+---+-------------------+-------------------+
|  0|0.42832091621126417| 0.7631927421386275|
|  1|0.23569963533458793|0.25355804372637497|
|  2|0.16271688773966747|0.13879624072961128|
|  3|0.12081039584460962| 0.8596739395534053|
|  4|0.12422772538137755|  0.531033191501185|
|  5| 0.6106904971676039| 0.3182094743880999|
|  6|  0.343409977029047| 0.3350436961031686|
|  7| 0.8246226780615837|0.06921905401733264|
|  8| 0.8718698988819364| 0.3138551509978246|
|  9| 0.8718698988819364| 0.8596739395534053|
|  0|0.42832091621126417| 0.7631927421386275|
|  1|0.23569963533458793|0.25355804372637497|
|  2|0.16271688773966747|0.13879624072961128|
|  3|0.12081039584460962| 0.8596739395534053|
|  4|0.12422772538137755|  0.531033191501185|
|  5| 0.6106904971676039| 0.3182094743880999|
|  6|  0.343409977029047| 0.3350436961031686|
|  7| 0.8246226780615837|0.06921905401733264|
|  8| 0.8718698988819364| 0.3138551509978246|
|  9| 0.8718698988819364| 0.8596739395534053|
+---+-------------------+-------------------+
     */
    retDF
  }
  
  
  /**
    * Returns splits for decision tree calculation.
    * Continuous and categorical features are handled differently.
    *
    * Continuous features:
    *   For each feature, there are numBins - 1 possible splits representing the possible binary
    *   decisions at each node in the tree.
    *   This finds locations (feature values) for splits using a subsample of the data.
    *
    * Categorical features:
    *   all distinct value
    *
    * @param maxBins:int,最大分桶数
    * @param numExamples:Long,样本数
    * @param continuousFeatures:Array[String],连续特征数组
    * @param input Training data: DataFrame
    * @param seed random seed
    * @return Splits, an Array of [[Double with String]]
    *          of size (numFeatures, numSplits)
    */
  def findSplits( maxBins:Int,
                  numExamples:Long,
                  continuousFeatures:Array[String],
                  input: DataFrame,
                  seed: Long): Array[Array[_ >: Double with String]] = {

    val numFeatures = input.columns.length
    // Sample the input only if there are continuous features.
    val fraction = samplesFractionForFindSplits(maxBins,numExamples)
    val sampledInput = input.sample(withReplacement = false, fraction, seed=888)

    findSplitsBySorting(maxBins,numExamples,numFeatures,sampledInput.toDF(), continuousFeatures)
  }


  def findSplitsBySorting( maxBins:Int,
                           numExamples:Long,
                           numFeatures:Int,
                           input: DataFrame,
                           continuousFeatures:Array[String]): Array[Array[_ >: Double with String]] = {

    val continuousSplits: scala.collection.Map[String, Array[Double]] = {
      // reduce the parallelism for split computations when there are less
      // continuous features than input partitions. this prevents tasks from
      // being spun up that will definitely do no work.
      val numPartitions = math.min(continuousFeatures.length, input.rdd.partitions.length)

      input.rdd
        .flatMap { row =>
          continuousFeatures.map(col => (col,row.getAs[Double](col))).filter(_._2!= 0.0)
        }
        .groupByKey(numPartitions)
        .map { case (col, samples) =>
          val thresholds = findSplitsForContinuousFeature(numExamples,maxBins,continuousFeatures,samples, col)
          (col, thresholds)
        }.collectAsMap()
    }

    val splits: Array[Array[_ >: Double with String]] = Array.tabulate(numFeatures) {
      case i if continuousFeatures.contains(input.columns(i)) =>
        // some features may contain only zero, so continuousSplits will not have a record
        val split = continuousSplits.getOrElse(input.columns(i), Array.empty[Double])
        split

      case i if !continuousFeatures.contains(input.columns(i)) =>
        //  split is all distinct val
        val split = input.rdd.map(row => row(i).toString).distinct().collect()
        split
    }
    splits
  }

  /**
    * Find splits for a continuous feature
    * NOTE: Returned number of splits is set based on `featureSamples` and
    *       could be different from the specified `numSplits`.
    *       The `numSplits` attribute in the `DecisionTreeMetadata` class will be set accordingly.
    *
    * @param numExamples:Long,样本数
    * @param maxBins:int,最大分桶数
    * @param continuousFeatures:Array[String],连续特征数组
    * @param featureSamples:Iterable[Double],同一列的所有特征值
    * @param featureColName:String,特征列名称
    * @return array of split thresholds,Array[Double]
    */
    def findSplitsForContinuousFeature( numExamples:Long,
                                        maxBins:Int,
                                        continuousFeatures:Array[String],
                                        featureSamples: Iterable[Double],
                                        featureColName: String): Array[Double] = {

    val splits: Array[Double] = if (featureSamples.isEmpty) {
      Array.empty[Double]
    } else {
      val numSplits = maxBins

      // get count for each distinct value except zero value
      val partNumSamples = featureSamples.size
      val partValueCountMap = scala.collection.mutable.Map[Double, Int]()
      featureSamples.foreach { x =>
        partValueCountMap(x) = partValueCountMap.getOrElse(x, 0) + 1
      }

      // Calculate the expected number of samples for finding splits
      val numSamples = (samplesFractionForFindSplits(maxBins,numExamples) * numExamples).toInt
      // add expected zero value count and get complete statistics
      val valueCountMap: Map[Double, Int] = if (numSamples - partNumSamples > 0) {
        partValueCountMap.toMap + (0.0 -> (numSamples - partNumSamples))
      } else {
        partValueCountMap.toMap
      }

      // sort distinct values
      val valueCounts = valueCountMap.toSeq.sortBy(_._1).toArray

      val possibleSplits = valueCounts.length - 1
      if (possibleSplits == 0) {
        // constant feature
        Array.empty[Double]
      } else if (possibleSplits <= numSplits) {
        // if possible splits is not enough or just enough, just return all possible splits
        (1 to possibleSplits)
          .map(index => (valueCounts(index - 1)._1 + valueCounts(index)._1) / 2.0)
          .toArray
      } else {
        // stride between splits
        val stride: Double = numSamples.toDouble / (numSplits + 1)
        print("debug stride = " + stride)

        // iterate `valueCount` to find splits
        val splitsBuilder = ArrayBuilder.make[Double]
        var index = 1
        // currentCount: sum of counts of values that have been visited
        var currentCount = valueCounts(0)._2
        // targetCount: target value for `currentCount`.
        // If `currentCount` is closest value to `targetCount`,
        // then current value is a split threshold.
        // After finding a split threshold, `targetCount` is added by stride.
        var targetCount = stride
        while (index < valueCounts.length) {
          val previousCount = currentCount
          currentCount += valueCounts(index)._2
          val previousGap = math.abs(previousCount - targetCount)
          val currentGap = math.abs(currentCount - targetCount)
          // If adding count of current value to currentCount
          // makes the gap between currentCount and targetCount smaller,
          // previous value is a split threshold.
          if (previousGap < currentGap) {
            splitsBuilder += (valueCounts(index - 1)._1 + valueCounts(index)._1) / 2.0
            targetCount += stride
          }
          index += 1
        }

        splitsBuilder.result()
      }
    }
    splits
  }

  /**
    * Calculate the subsample fraction for finding splits
    *
    * @param maxBins:int,最大分桶数
    * @param numExamples:Long,样本数
    * @return subsample fraction
    */
   def samplesFractionForFindSplits(maxBins:Int,numExamples:Long): Double = {
    // Calculate the number of samples for approximate quantile calculation.
    val requiredSamples = math.max(maxBins * maxBins, 10000)
    if (requiredSamples < numExamples) {
      requiredSamples.toDouble / numExamples
    } else {
      1.0
    }
  }

  /**
    * 有序数组寻找元素上界
    * @param value
    * @param array
    * @return Double
    */
  def find(value:Double,array: Array[Double]):Double = {
    var ret = array(0)
    if( value<array(0) ){
      array(0)
    }
    if( value>array.last ){
      ret = array.last
    }
    for(idx <- 0 until array.length-1){
      if( array(idx) <= value && array(idx+1)>= value ){
        ret = array(idx+1)
      }
    }
    ret
  }

}
