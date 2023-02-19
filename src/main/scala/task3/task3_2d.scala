package task3

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

case class Point_Dom(x: Double, y: Double, var dominating_score: Long)
{
  def dominates(point: Point_Dom): Boolean = x < point.x && y < point.y
}



object task3_2d {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder().master("local").appName("TopKSkylineDominantPoints").getOrCreate()
    val sparkConfig = new SparkConf()
      .setMaster("local")
      .setAppName("TopKDominantPoints")

    // create spark context
    val sc =  SparkContext.getOrCreate(sparkConfig)

    val pointsDF =  ss.read.option("header", "true").csv("/home/ozzy/IdeaProjects/test/uniform_2_10000000.csv")
    val doubleDF = pointsDF.select(pointsDF.columns.map(c => pointsDF(c).cast("double")): _*)

    val points = doubleDF.rdd.map(row => Point_Dom(row.getDouble(0), row.getDouble(1), 0))

    val startTime_task2 = System.currentTimeMillis()



    def skyline(points: RDD[Point_Dom], blockSize: Int): RDD[Point_Dom] = {
      val blocks = points.map(p => (math.floor(p.x / blockSize), p))
        .groupByKey()
        .mapValues(_.toList.sortBy(_.y))
      val sky = blocks.mapValues(block => {
        var sky = List[Point_Dom]()
        for (p <- block) {
          if (!sky.exists(q => q.y <= p.y && q.x < p.x)) {
            sky = sky.filter(q => q.y < p.y || q.x <= p.x)
            sky = p :: sky
          }
        }
        sky
      }).values.flatMap(x => x)
      sky
    }

    def getDominations(pointsRDD: RDD[Point_Dom], pointsBuffer: ArrayBuffer[Point_Dom] ) {
      for(point <- pointsBuffer){
        point.dominating_score = pointsRDD.filter(p => p.x > point.x && p.y > point.y).count()
      }
    }

    def Top_K_SkylineDominators(points: RDD[Point_Dom], k: Int): ArrayBuffer[Point_Dom] = {

      val original_skyline = skyline(points = points, 100)

      var skylineArrayBuffer = original_skyline.collect().to[ArrayBuffer]

      getDominations(points, skylineArrayBuffer)

      skylineArrayBuffer = skylineArrayBuffer.sortBy(_.dominating_score).reverse

      skylineArrayBuffer.take(k)
    }

    val results = Top_K_SkylineDominators(points,5)

    val endTime_task2 = System.currentTimeMillis()
    val elapsedTime_task2 = endTime_task2 - startTime_task2

    for (x <- results) {
      println(x)
    }
    println(s"Elapsed time for Task 1: $elapsedTime_task2 ms")
  }
}