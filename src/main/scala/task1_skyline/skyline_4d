package task1_skyline
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD._



object skyline_4d {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder().master("local").appName("skylines").getOrCreate()

    val pointsDF =  ss.read.option("header", "true").csv("/home/ozzy/Documents/anticorrelated_4d_1000.csv")
    val doubleDF = pointsDF.select(pointsDF.columns.map(c => pointsDF(c).cast("double")): _*)



    //val points_unpartitioned = doubleDF.rdd.map(row => Point(for (i <- 0 until 3) yield row.getDouble(i)))
    val points_unpartitioned = doubleDF.rdd.map(row => Point4d(row.getDouble(0), row.getDouble(1), row.getDouble(2), row.getDouble(3)))
    val points = points_unpartitioned.repartition(1)

    //val partitions = points.partitions.length
    //println(s"Number of partitions: $partitions")


    def skyline(points: RDD[Point4d], blockSize: Int): RDD[Point4d] = {
      val blocks = points.map(p => (math.floor(p.d1 / blockSize), p))
        .groupByKey()
        .mapValues(_.toList.sortBy(p => (p.d2, p.d3, p.d4)))

      val sky = blocks.mapValues(block => {
        var sky = List[Point4d]()
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

    val start = System.nanoTime
    val found_skyline = skyline(points = points, 100)

    found_skyline.foreach(println)
    println("Total skylines: " + found_skyline.count())

    val totaltime = System.nanoTime - start
    println("Total runtime: " + totaltime.asInstanceOf[Double] / scala.math.pow(10,9) + " sec")
  }
}
