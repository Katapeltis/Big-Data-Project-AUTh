package dataframe_solutions

import org.apache.spark.sql.functions.{filter, col, lit} 

import org.apache.spark.sql.types.{StringType, StructField, StructType} 

import org.apache.spark.sql.SparkSession 

  

object df_import_func{ 

  

  def main(args: Array[String]): Unit = { 

  

    val d = 2 

    val N = 10 

    // val cols = (0 until d - 1).map(_.toString) 

  

    val spark = SparkSession 

      .builder() 

      .config("spark.master", "local") 

      .getOrCreate() 

  

  

    val dir = "/home/ozzy/Documents" 

  

    var df = spark.read.format(source="csv") 

      .option("sep", ",") 

      .option("inferSchema", "true") 

      .option("header", "true") 

      .load(path = "file://" + dir + "/normal_2d_10N.csv") 

  

  

  

    val dominanceScores = Array.fill(N)(0) 

    for (i <- 0 until N) { 

      for (j <- 0 until N) { 

        if (i != j) { 

          val comparison = col(s"$i") < col(s"$j") 

          if (df.filter(comparison).count() > d-1) { 

            dominanceScores(i) += 1 

          } 

        } 

      } 

    } 

    df = df.drop("comparison") 

  

    df.show() 

    dominanceScores.foreach(score => println(score)) 

  } 

} 
