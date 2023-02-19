package task2

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import task1.Point4d

import scala.collection.mutable.ArrayBuffer


case class Point4d_Dom(d1: Double, d2: Double, d3: Double, d4: Double, var dominating_score: Long)
{
  def dominates(point: Point4d_Dom): Boolean = d1 < point.d1 && d2 < point.d2
}


object task2_4d {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder().master("local").appName("TopKDominantPoints").getOrCreate()
    val sparkConfig = new SparkConf()
      .setMaster("local")
      .setAppName("TopKDominantPoints")

    // create spark context
    val sc =  SparkContext.getOrCreate(sparkConfig)

    val pointsDF =  ss.read.option("header", "true").csv("/home/ozzy/IdeaProjects/test/anticorrelated_4_100000.csv")
    val doubleDF = pointsDF.select(pointsDF.columns.map(c => pointsDF(c).cast("double")): _*)

    val points = doubleDF.rdd.map(row => Point4d_Dom(row.getDouble(0), row.getDouble(1), row.getDouble(2), row.getDouble(3), 0))

    val startTime_task2 = System.currentTimeMillis()

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
        point.dominating_score = pointsRDD.filter(p => p.d1 > point.d1 && p.d2 > point.d2).count()
      }
    }

    def Top_K_Dominators(points: RDD[Point4d_Dom], k: Int): ArrayBuffer[Point4d_Dom] = {

      var topKdominators = new ArrayBuffer[Point4d_Dom]()
      var points_mutable = points
      var dominators_count = 0

      while (dominators_count<k) {
        //print(topDominators)
        val current_skyline = skyline(points = points_mutable, 100)
        //current_skyline.foreach(println)
        var skylineArrayBuffer = current_skyline.collect().to[ArrayBuffer]

        getDominations(points, skylineArrayBuffer)

        skylineArrayBuffer = skylineArrayBuffer.sortBy(_.dominating_score).reverse

        val topPoint = skylineArrayBuffer(0)
        topKdominators += topPoint

        points_mutable = points_mutable.filter(x => x.d1 != topPoint.d1 && x.d2 != topPoint.d2 && x.d3 != topPoint.d3 && x.d4 != topPoint.d4)
        dominators_count+=1
      }
      topKdominators
    }

    val results = Top_K_Dominators(points,5)

    val endTime_task2 = System.currentTimeMillis()
    val elapsedTime_task2 = endTime_task2 - startTime_task2

    for (x <- results) {
      println(x)
    }
    println("Total runtime: " + elapsedTime_task2 / scala.math.pow(10,3) + " sec")
  }
}