package org.ics.isl

import org.apache.spark.sql.SparkSession
import sys.process._


object Preprocessor {


  def toUri(uri: String): String = {"<" + uri + ">"}

  //return rdd(s,p,o)
  def loadTriples(spark: SparkSession, dataset: String, hdfs: String, datasetPath: String) = {
    val sc = spark.sparkContext
    println("loadTriples:"+datasetPath)
    sc.textFile(datasetPath)
      .filter(!_.startsWith("#"))
      //.filter(_.startsWith("<"))
      .map(_.split("\\s+", 3))
      .map(t => (t(0).trim, t(1).trim, t(2).dropRight(2).trim))
      .distinct()
  }


  def printTriples(spark: SparkSession, dataset: String, hdfs: String, datasetPath: String): Unit ={
    val sc = spark.sparkContext
    println("printTriples:"+datasetPath)
    sc.textFile(datasetPath)
      //.filter(_.startsWith("<"))
      .map(_.split("\\s+", 3)).collect()
      .foreach(t => println(t(2).dropRight(2).trim +" - "+t.length))
  }


  def createDataset(spark: SparkSession, dataset: String, hdfs: String, datasetPath: String) = {
    import spark.implicits._
    val datasetRdd = loadTriples(spark, dataset, hdfs, datasetPath)

    //datasetRdd.toDF().write.format("parquet").save(hdfs + dataset + Constants.clusters + Constants.dataset)

    datasetRdd.toDF.as[(String, String, String)]
              .write.parquet(hdfs + dataset + Constants.clusters + Constants.dataset)
  }


  def getDirFilesNames(path: String): Array[String] = {
    val lsCmd =  "hdfs" + " dfs -ls " + path
    val cmd: String = lsCmd.!!
    var fileNames = Array[String]()
    cmd.split("\n").drop(1).filter(!_.contains("SUCCESS")).foreach(line => {
      fileNames = fileNames :+ line.substring(line.lastIndexOf("/")+1)
    })
    fileNames
  }

  def roundUp(d: Double) = math.ceil(d).toInt

  def uriToStorage(uri: String) = {
    uri.replaceAll("[^A-Za-z0-9]", "_").toLowerCase
  }


  def createLevels(spark: SparkSession, dataset: String, hdfs: String, levelsPath: String) = {
    import spark.implicits._
    val levels = getDirFilesNames(levelsPath)

    levels.foreach(level => {
      println("level: "+ level)
      println("level.dropRight: "+ level.dropRight(3))
      val fullPath = levelsPath + "/" + level
      println("fulpath: "+ fullPath)

      //printTriples(spark, dataset, hdfs, fullPath)
      val levelRdd = loadTriples(spark, dataset, hdfs, fullPath)//.toDF.as[(String, String, String)]

      //store tables with three columns (s,p,o)
      /*levelRdd.toDF.as[(String, String, String)]
        .write.parquet(hdfs + dataset + Constants.clusters + Constants.level + level.dropRight(3))
      */

      //store VP tables with two columns (s,o)
      val partSize = levelRdd.partitions.size
      levelRdd.repartition(roundUp(partSize/7.0)) //partition.rdd.repartition(roundUp(partSize/7.0))
        .filter{case(s, p, o) => p.size < 150}
        .toDF.as[(String, String, String)]
        .map{case(s, p, o) => (s, o, uriToStorage(p))}
        .write.partitionBy("_3").parquet(hdfs + dataset + Constants.clusters + Constants.level + level.dropRight(3)) //create folders for each predicate inside to each folder of cluster

    })

  }



}
