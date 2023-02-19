package task1_skyline
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD._


object skyline_10d {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder().master("local").appName("skylines").getOrCreate()

    val pointsDF =  ss.read.option("header", "true").csv("/home/ozzy/Documents/normal_10d_1000.csv")
    val doubleDF = pointsDF.select(pointsDF.columns.map(c => pointsDF(c).cast("double")): _*)




    val points_unpartitioned = doubleDF.rdd.map(row => Point10d(row.getDouble(0), row.getDouble(1), row.getDouble(2),
      row.getDouble(3), row.getDouble(4), row.getDouble(5), row.getDouble(6), row.getDouble(7), row.getDouble(8),
      row.getDouble(9)))
    val points = points_unpartitioned.repartition(1)


    def skyline(points: RDD[Point10d], blockSize: Int): RDD[Point10d] = {
      val blocks = points.map(p => (math.floor(p.d1 / blockSize), p))
        .groupByKey()
        .mapValues(_.toList.sortBy(p => (p.d2, p.d3, p.d4, p.d5, p.d6, p.d7, p.d8, p.d9, p.d10)))

      val sky = blocks.mapValues(block => {
        var sky = List[Point10d]()
        for (p <- block) {
          if (!sky.exists(q => q.d2 <= p.d2 && q.d1 < p.d1 && q.d3 < p.d3 && q.d4 < p.d4
            && q.d5 < p.d5 && q.d6 < p.d6 && q.d7 < p.d7 && q.d8 < p.d8 && q.d9 < p.d9 && q.d10 < p.d10)) {
            sky = sky.filter(q => q.d2 < p.d2 || q.d1 <= p.d1 || q.d3 <= p.d3 || q.d4 <= p.d4
              || q.d5 <= p.d5 || q.d6 <= p.d6 || q.d7 <= p.d7 || q.d8 <= p.d8 || q.d9 <= p.d9 || q.d10 <= p.d10)
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
