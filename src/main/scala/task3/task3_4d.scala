package task3

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import task2.Point4d_Dom

import scala.collection.mutable.ArrayBuffer

case class Point4d_Dom(d1: Double, d2: Double, d3: Double, d4: Double, var dominating_score: Long)
{
  def dominates(point: Point4d_Dom): Boolean = d1 < point.d1 && d2 < point.d2
}



object task3_4d {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder().master("local").appName("TopKSkylineDominantPoints").getOrCreate()
    val sparkConfig = new SparkConf()
      .setMaster("local")
      .setAppName("TopKDominantPoints")

    // create spark context
    val sc =  SparkContext.getOrCreate(sparkConfig)

    val pointsDF =  ss.read.option("header", "true").csv("/home/ozzy/IdeaProjects/test/anticorrelated_4_1000000.csv")
    val doubleDF = pointsDF.select(pointsDF.columns.map(c => pointsDF(c).cast("double")): _*)

    val points = doubleDF.rdd.map(row => Point4d_Dom(row.getDouble(0), row.getDouble(1), row.getDouble(2), row.getDouble(3), 0))

    val startTime_task3 = System.currentTimeMillis()

    def skyline(points: RDD[Point4d_Dom], blockSize: Int): RDD[Point4d_Dom] = {
      val blocks = points.map(p => (math.floor(p.d1 / blockSize), p))
        .groupByKey()
        .mapValues(_.toList.sortBy(p => (p.d2, p.d3, p.d4)))

      val sky = blocks.mapValues(block => {
        var sky = List[Point4d_Dom]()
        for (p <- block) {
          if (!sky.exists(q => q.d2 <= p.d2 && q.d1 < p.d1 && q.d3 < p.d3 && q.d4 < p.d4)) {
            sky = sky.filter(q => q.d2 < p.d2 || q.d1 <= p.d1 || q.d3 <= p.d3 || q.d4 <= p.d4)
            sky = p :: sky
          }
        }
        sky
      }).values.flatMap(x => x)
      sky
    }

    def getDominations(pointsRDD: RDD[Point4d_Dom], pointsBuffer: ArrayBuffer[Point4d_Dom] ) {
      for(point <- pointsBuffer){
        point.dominating_score = pointsRDD.filter(p => p.d1 > point.d1 && p.d2 > point.d2 && p.d3 > point.d3 && p.d4 > point.d4).count()
      }
    }

    def Top_K_SkylineDominators(points: RDD[Point4d_Dom], k: Int): ArrayBuffer[Point4d_Dom] = {

      val original_skyline = skyline(points = points, 100)

      var skylineArrayBuffer = original_skyline.collect().to[ArrayBuffer]

      getDominations(points, skylineArrayBuffer)

      skylineArrayBuffer = skylineArrayBuffer.sortBy(_.dominating_score).reverse

      skylineArrayBuffer.take(k)
    }

    val results = Top_K_SkylineDominators(points,5)

    val endTime_task3 = System.currentTimeMillis()
    val elapsedTime_task2 = endTime_task3 - startTime_task3

    for (x <- results) {
      println(x)
    }
    println("Elapsed time for Task 1: " + elapsedTime_task2 / scala.math.pow(10,3) + "ms")
  }
}