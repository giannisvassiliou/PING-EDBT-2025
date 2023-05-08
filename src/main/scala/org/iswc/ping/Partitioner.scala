package org.iswc.ping

import org.apache.spark.sql.SparkSession

object Partitioner {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("BLAP")
      .config("master", "mesos://zk://clusternode1:2181,clusternode2:2181,clusternode3:2181/mesos")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "1024m")
      .config("spark.driver.maxResultSize", "10G")
      .config("spark.hadoop.dfs.replication", "1")
      .getOrCreate()

    val sc = spark.sparkContext

    var dataset = args(0)
    var hdfs = args(1)
    var instancePath = args(2)
    var levelsPath = args(3)

    if (args.size < 3) {
      println("Missing Arguments")
      println("Arguments: number of partitions, dataset name, hdfs base path, insances path, schema path")
      System.exit(-1)
    }

    if(!hdfs.endsWith("/"))
      hdfs = hdfs + "/"

    sc.setCheckpointDir(hdfs + "checkPoint")
    sc.setLogLevel("ERROR")

    Preprocessor.createDataset(spark, dataset, hdfs, instancePath)
    Preprocessor.createLevels(spark, dataset, hdfs, levelsPath)


    spark.stop()

  }



}
