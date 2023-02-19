package dataframe_solutions

import org.apache.spark.sql.functions.{filter, col, lit} 

import org.apache.spark.sql.types.{StringType, StructField, StructType} 

import org.apache.spark.sql.SparkSession 

  

object df_import_func{ 

  

  def main(args: Array[String]): Unit = { 

  

    val d = 2 

    val N = 50 

  

    val spark = SparkSession 

      .builder() 

      .config("spark.master", "local") 

      .getOrCreate() 

  

  

    val dir = "/home/ozzy/Documents" 

  

    var df = spark.read.format(source="csv") 

      .option("sep", ",") 

      .option("inferSchema", "true") 

      .option("header", "true") 

      .load(path = "file://" + dir + "/normal_2d_50N.csv") 

  

    var df2 = df 

  

     

    for (i <- 0 until N) { 

      if (!(df.filter(col(i.toString) === 0).count() > 0)) { 

        for (j <- 0 until N) { 

          if (df.filter(col(j.toString) === 0).count() == 0 && i != j) { 

            df = df.withColumn("comparison", col(i.toString) < col(j.toString)) 

            if (df.filter(col("comparison") === true).count() > d - 1) { 

              df = df.withColumn(j.toString, lit(0)) 

            } 

          } 

        } 

      } 

    } 

  

    df = df.drop("comparison") 

  

    var df_skyline = df 

    for (i <- 0 until N) { 

      if (df.filter(col(i.toString) === 0).count() == d) { 

        df_skyline = df_skyline.drop(i.toString) 

      } 

    } 

  

  

    val skylines = df_skyline.columns.map(x => x.toInt) 

    val dominanceScores = Array.fill(N)(0) 

  

    for (i <- skylines) { 

      for (j <- 0 until N) { 

        if (i != j) { 

          val comparison = col(s"$i") < col(s"$j") 

          if (df2.filter(comparison).count() > d-1) { 

            dominanceScores(i) += 1 

          } 

        } 

      } 

    } 

    df2 = df.drop("comparison") 

  

    df2.show() 

    dominanceScores.foreach(score => println(score)) 

  } 

} 
