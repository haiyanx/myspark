package com.splunk.breeze

import breeze.linalg._
import breeze.numerics._
import org.apache.log4j.{ Level,Logger}
import org.apache.spark.{ SparkConf,SparkContext}

object BreezeDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD").setMaster("local")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    // 3.1.1 Breeze 创建函数
    val m1: DenseMatrix[Double] = DenseMatrix.zeros[Double](2,3)
    //    0.0  0.0  0.0   矩阵
    //    0.0  0.0  0.0
    val v1: DenseVector[Double] = DenseVector.zeros[Double](3)
    //    DenseVector(0.0, 0.0, 0.0) 向量
    val v2: DenseVector[Double] = DenseVector.ones[Double](3)
    //    DenseVector(1.0, 1.0, 1.0)
    val v3: DenseVector[Double] = DenseVector.fill(3){5.0}
    //    DenseVector(5.0, 5.0, 5.0)
    val v4: DenseVector[Int] = DenseVector.range(1,10,2)
    //    DenseVector(1, 3, 5, 7, 9)
    val m2: DenseMatrix[Double] = DenseMatrix.eye[Double](3)
    //    1.0  0.0  0.0
    //    0.0  1.0  0.0
    //    0.0  0.0  1.0
    val v6: DenseMatrix[Double] = diag(DenseVector(1.0,2.0,3.0))
    //    1.0  0.0  0.0
    //    0.0  2.0  0.0
    //    0.0  0.0  3.0
    val m3: DenseMatrix[Double] = DenseMatrix((1.0,2.0),(3.0,4.0))
    //    1.0  2.0
    //    3.0  4.0
    val v8: DenseVector[Int] = DenseVector(1,2,3,4)
    // DenseVector(1, 2, 3, 4)
    val v9: Transpose[DenseVector[Int]] = DenseVector(1,2,3,4).t
    //    Transpose(DenseVector(1, 2, 3, 4))
    val v10: DenseVector[Int] = DenseVector.tabulate(3){ i => 2*i}
//    DenseVector(0, 2, 4)



    println(v10)

  }
}
