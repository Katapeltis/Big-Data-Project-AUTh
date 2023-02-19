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

      .load(path = "file://" + dir + "/normal.csv") 

  

  

  

    for (i <- 0 until N) { 

      if (!(df.filter(col(i.toString) === 0).count() > 0)) { 

        println(i) 

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

  

    for (i <- 0 until N) { 

      if (df.filter(col(i.toString) === 0).count() == d) { 

        df = df.drop(i.toString) 

      } 

    } 

    df.show() 

  } 

} 
