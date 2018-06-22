package org.apache.spark

import org.apache.spark.sql.functions.rand
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}

import scala.collection.mutable.ArrayBuilder

/**
  * 提供互信息相关性分析中的内置离散化方法
  * Created by tongzhenguo on 2018/6/14.
  * desc:改写spark决策树的分割算法为互信息相关性分析法的内置离散化方法
  * 需求分析：
  * 输入：input：DataFrame,maxBins:int,最大特征数，要求大于2,建议值32,且大于等于字符串型列独立值个数，由于抽样样本最多1w，maxBins也不超过一万
  * 输出：output：DataFrame,以上界分割点或者字符串枚举为值重新构建的一个数据框
  * 实现：
  * 测试：
  * 1.基本功能 解决
  * 2.当连续列独立值小于maxBin，期待直接离散化值就是原值 解决
  * 3.类别型取值范围较大,可能超出10000种独立值 abs(hash(col) % 10000)
  */
object FindSplit {

  def getSplitResult(inputDF: DataFrame, maxBins: Int = 32): DataFrame = {
    var input = inputDF
    // 统计类别型独立值个数，验证maxBins是否正确
    var categoryMaxDistinCount = 0L
    var continuousFeatures = Array[String]()
    for ((colname, coltype) <- input.dtypes) {

      if ("StringType".equals(coltype)) {
        var featValCount = input.select(colname).distinct().count()
        if (featValCount > categoryMaxDistinCount) {
          if (featValCount > 10000) {
            featValCount = 10000L
            val exprFormat = "cast(abs(hash(" + colname + ") % 10000) as varchar)"
            input = input.withColumn("hash_%s".format(colname), functions.expr(exprFormat))
            input = input.drop(colname).withColumnRenamed("hash_%s".format(colname), colname)
          }
          categoryMaxDistinCount = featValCount
        }
      } else { // all convert to double type
        input = input.withColumn("cast_%s".format(colname), functions.expr("cast(%s as double)".format(colname)))
        input = input.drop(colname).withColumnRenamed("cast_%s".format(colname), colname)
        continuousFeatures = continuousFeatures :+ colname
      }
    }

    if (maxBins < categoryMaxDistinCount) {
      val formatMsg = "maxBins must more than category column maximum distinct value count,%s<%s"
        .format(maxBins, categoryMaxDistinCount)
      throw new IllegalArgumentException(formatMsg)
    }
    if (maxBins > 100000) {
      val formatMsg = "maxBins must less than sample example count,%s>%s".format(maxBins, 10000)
      throw new IllegalArgumentException(formatMsg)
    }

    // 改造findSplits,并调用之，根据结果构造output
    val schema = input.schema
    val columns = input.columns
    val numExamples = input.count()
    var splits: Array[Array[_ >: Double with String]] = findSplits(maxBins, numExamples, continuousFeatures, input, seed = 888)

    println("show splits:")
    for ((fields, idx) <- splits.zipWithIndex) {
      println("%s:%s".format(input.columns(idx), fields.mkString(",")))
    }

    // 以分割点替换特征值
    val retRDD = input.rdd.map(row => {
      var newRow = Array[Any]()
      for (idx <- 0 until row.length) {
        val newVal = if (!continuousFeatures.contains(columns(idx))) {
          row(idx).toString
        } else if (splits(idx).length < maxBins) {
          // 如果连续值独立数小于maxBins,直接返回原值
          row(idx).toString.toDouble
        } else {
          // 查找分割点
          find(row(idx).toString.toDouble, splits(idx).map(_.toString.toDouble))
        }
        newRow = newRow :+ newVal
      }
      Row(newRow: _*)
    })
    val retDF = input.sparkSession.createDataFrame(retRDD, input.schema)
    retDF.show()
    retDF
  }


  /**
    * Returns splits for decision tree calculation.
    * Continuous and categorical features are handled differently.
    *
    * Continuous features:
    * For each feature, there are numBins - 1 possible splits representing the possible binary
    * decisions at each node in the tree.
    * This finds locations (feature values) for splits using a subsample of the data.
    *
    * Categorical features:
    * all distinct value
    *
    * @param maxBins            :int,最大分桶数
    * @param numExamples        :Long,样本数
    * @param continuousFeatures :Array[String],连续特征数组
    * @param input              Training data: DataFrame
    * @param seed               random seed
    * @return Splits, an Array of [[Double with String]]
    *         of size (numFeatures, numSplits)
    */
  def findSplits(maxBins: Int,
                 numExamples: Long,
                 continuousFeatures: Array[String],
                 input: DataFrame,
                 seed: Long): Array[Array[_ >: Double with String]] = {

    val numFeatures = input.columns.length
    // Sample the input only if there are continuous features.
    val fraction = samplesFractionForFindSplits(maxBins, numExamples)
    val sampledInput = input.sample(withReplacement = false, fraction, seed = 888)

    findSplitsBySorting(maxBins, numExamples, numFeatures, sampledInput.toDF(), continuousFeatures)
  }


  def findSplitsBySorting(maxBins: Int,
                          numExamples: Long,
                          numFeatures: Int,
                          input: DataFrame,
                          continuousFeatures: Array[String]): Array[Array[_ >: Double with String]] = {

    val continuousSplits: scala.collection.Map[String, Array[Double]] = {
      // reduce the parallelism for split computations when there are less
      // continuous features than input partitions. this prevents tasks from
      // being spun up that will definitely do no work.
      val numPartitions = math.min(continuousFeatures.length, input.rdd.partitions.length)

      input.rdd
        .flatMap { row =>
          continuousFeatures.map(col => (col, row.getAs[Double](col))) //.filter(_._2!= 0.0)
        }
        .groupByKey(numPartitions)
        .map { case (col, samples) =>
          val thresholds = findSplitsForContinuousFeature(numExamples, maxBins, continuousFeatures, samples, col)
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
    * could be different from the specified `numSplits`.
    * The `numSplits` attribute in the `DecisionTreeMetadata` class will be set accordingly.
    *
    * @param numExamples        :Long,样本数
    * @param maxBins            :int,最大分桶数
    * @param continuousFeatures :Array[String],连续特征数组
    * @param featureSamples     :Iterable[Double],同一列的所有特征值
    * @param featureColName     :String,特征列名称
    * @return array of split thresholds,Array[Double]
    */
  def findSplitsForContinuousFeature(numExamples: Long,
                                     maxBins: Int,
                                     continuousFeatures: Array[String],
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
      val numSamples = (samplesFractionForFindSplits(maxBins, numExamples) * numExamples).toInt
      // add expected zero value count and get complete statistics
      val valueCountMap: Map[Double, Int] = if (numSamples - partNumSamples > 0) {
        partValueCountMap.toMap + (0.0 -> (numSamples - partNumSamples))
      } else {
        partValueCountMap.toMap
      }

      // sort distinct values
      val valueCounts = valueCountMap.toSeq.sortBy(_._1).toArray

      val possibleSplits = valueCounts.length - 1
      if (valueCounts.length <= maxBins) {
        // if distinct value is not enough or just enough, just return all distinct value
        (0 until valueCounts.length)
          .map(index => valueCounts(index)._1)
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
    * @param maxBins     :int,最大分桶数
    * @param numExamples :Long,样本数
    * @return subsample fraction
    */
  def samplesFractionForFindSplits(maxBins: Int, numExamples: Long): Double = {
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
    *
    * @param value
    * @param array
    * @return Double
    */
  def find(value: Double, array: Array[Double]): Double = {
    var ret = array(0)
    if (value < array(0)) {
      array(0)
    }
    if (value > array.last) {
      ret = array.last
    }
    for (idx <- 0 until array.length - 1) {
      if (array(idx) <= value && array(idx + 1) >= value) {
        ret = array(idx + 1)
      }
    }
    ret
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .getOrCreate()
    import spark.implicits._

    // 测试离散化分区方法
    var input: DataFrame = spark.sparkContext.parallelize(
      0 until 100000).map(_.toString).toDF(
      "id").withColumn(
      "a", rand(seed = 10)
    ).withColumn("b", rand(seed = 27)
    )
    val input2: DataFrame = spark.sparkContext.parallelize(
      0 until 100000).map(_.toString).toDF(
      "id").withColumn(
      "a", rand(seed = 15)
    ).withColumn("b", rand(seed = 37)
    )

    input = input.union(input2)
    input.createOrReplaceTempView("spark_tmp_table")
    val sql =
      """
        |select id,a,b,case when b > 0.5 then 1 else 0 end as c
        |from spark_tmp_table
      """.stripMargin
    input = spark.sql(sql)

    // getSplitResult(input,maxBins = 10) // maxBins must more than category column maximum distinct value count,10<100000
    getSplitResult(input, maxBins = 10000)

  }

}
