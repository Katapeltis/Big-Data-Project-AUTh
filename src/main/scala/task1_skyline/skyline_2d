package task1_skyline
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD._


object skyline_2d {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder().master("local").appName("skylines").getOrCreate()

    val pointsDF =  ss.read.option("header", "true").csv("/home/ozzy/Documents/normal_2d_20N.csv")
    val doubleDF = pointsDF.select(pointsDF.columns.map(c => pointsDF(c).cast("double")): _*)

    val points_unpartitioned = doubleDF.rdd.map(row => Point(row.getDouble(0), row.getDouble(1)))
    val points = points_unpartitioned.repartition(1)

    val partitions = points.partitions.length
    println(s"Number of partitions: $partitions")


    def skyline(points: RDD[Point], blockSize: Int): RDD[Point] = {
      val blocks = points.map(p => (math.floor(p.x / blockSize), p))
        .groupByKey()
        .mapValues(_.toList.sortBy(_.y))

      val sky = blocks.mapValues(block => {
        var sky = List[Point]()
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

    val start = System.nanoTime
    val found_skyline = skyline(points = points, 100)

    found_skyline.foreach(println)
    println("Total skylines: " + found_skyline.count())

    val totaltime = System.nanoTime - start
    println("Total runtime: " + totaltime.asInstanceOf[Double] / scala.math.pow(10,9) + " sec")
  }
}
