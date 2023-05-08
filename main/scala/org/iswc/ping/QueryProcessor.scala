package org.eswc.smarter

import scala.io.Source
import java.io._

import sys.process._
import scala.concurrent.duration._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession

import scala.collection.Map
import scala.collection.immutable.ListMap



/**
 * Query Processor Direct using SQL
 *
 */
object QueryProcessor {
  val spark = loadSparkSession()
  var dataMap = Map[String, Dataset[Row]]()
  var firstRun = 0
  var dataset = ""
  var hdfs = ""
  var selectedGraph = 0 //execute queries in original graph or levels

  /**
   * Initializes spark session
   */
  def loadSparkSession() = {

    val spark = SparkSession.builder.appName("QueryProcessor")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.inMemoryColumnarStorage.compressed", "true")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .config("spark.sql.parquet.filterPushdown", true)
      .config("spark.sql.inMemoryColumnarStorage.batchSize", "20000")
      .config("spark.sql.shuffle.partitions", "200")
      .config("spark.sql.cbo.enabled", true) // cost based optimizer
      .config("spark.sql.cbo.joinReorder.enabled", true) // cost based optimizer
      //.config("spark.sql.adaptive.enabled", true) //spark 3.0
      //.config("spark.sql.adaptive.skewJoin.enabled", true) //spark 3.0
      //.config("spark.sql.adaptive.localShuffleReader.enabled", true) //spark 3.0
      //.config("spark.sql.adaptive.coalescePartitions.enabled", true) //spark 3.0
      .getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    spark
  }

  def main(args: Array[String]): Unit = {
    import spark.implicits._

    if (args.size != 3) {
      println("Arguments: number of partitions, dataset name, bal, hdfs base path, input path")
      System.exit(-1)
    }

    dataset = args(0)
    selectedGraph = 1 //args(1).toInt // 0 (original graph) or 1 (levels)
    hdfs = args(1)
    var inputPath = args(2)

    if (!hdfs.endsWith("/"))
      hdfs = hdfs + "/"

    val sc = spark.sparkContext

    var resultPath = "results/" + dataset + "/"

    if(!new File(resultPath).exists){
      val mkdirBase = "mkdir results" !
      val mkdirDataset = "mkdir results/" + dataset !
     // val cmd = "mkdir " + resultPath !
    }
    println("inputPath:"+inputPath)
    val fileList = listFiles(new File(inputPath), true)

    fileList.foreach(queryFile => {
      val queryPath = queryFile.getPath
      val queryName = queryFile.getName

      //println("queryPath: "+queryPath)
      println("Query: " + queryName)

      //Parse query
      val file = Source.fromFile(queryFile).getLines

      var levels = Array[String]()
      var partitions = Map[String, Array[String]]()
      var tpNum: Int = -1
      var queryMap = Map[String, String] ()

      for(line <- file) {
        if(line.startsWith("TP")){
          tpNum = line.split(" ")(1).toInt
        }
        else if(line.startsWith("partitions")){
          var parts = line.split(" ")
          //var predicate = uriToStorage(parts(1))
          //println("parts1: " + parts(1))
          //println("part2: " + parts(2))
          //println("part2: " + parts(2).split(","))
          levels = parts(2).split(",").map("lv" + _) //parts(2): 2,3,4,5,6,7
          partitions = partitions + (parts(1) -> levels)  //parts(1): predicate
          //!todo: concat predicate +level
        }
        else if(!line.startsWith(">>>>>")) {
          val tokens = line.split(" ")
          val key = tokens(0)
          val query = tokens.drop(1).mkString(" ")
          //println("-key: "+key)  //X
          //println("query: "+query) //query
          queryMap = queryMap + (key -> query)
        }
      }

      val resultFile = new File(resultPath + "results" + "_" + tpNum + "_" + "bal" + ".txt")

      val resultWriter = new FileWriter(resultFile, true) //appends

      //println("Query: " + queryName)
      val (executionTime, loadTime, loadDataSize, result) = executeQuery(queryMap, partitions, dataset, queryName.dropRight(7)) //swdf: dropRight(4)

      /*if (selectedGraph == 2){
        var numOfLevels = partitions.map(x => (x._1, x._2.length))
        val (executionTime, loadTime, loadDataSize, result) = executeQuery(queryMap, partitions, dataset, queryName.dropRight(7)) //swdf: dropRight(4)
      }else {
        val (executionTime, loadTime, loadDataSize, result) = executeQuery(queryMap, partitions, dataset, queryName.dropRight(7)) //swdf: dropRight(4)
      }*/

      resultWriter.append("Query: " + queryFile.getName + "\n")
      resultWriter.append("Time: " + executionTime + "\n")
      resultWriter.append("Time_load: " + loadTime + "\n")
      resultWriter.append("Load_data: " + loadDataSize + "\n")
      resultWriter.append("Result_count: " + result.count + "\n")
      resultWriter.append("partitions: " + partitions.mkString(",") + "\n")

      //result.collect().foreach(println(_))

      println("Time: " + executionTime + "\n")

      resultWriter.close
    })

    spark.stop()
  }

  def uriToStorage(uri: String) = {
    uri.replaceAll("[^A-Za-z0-9]", "_").toLowerCase
  }


  /**
   * handles queries that do not have rdf:type
   */
  def executeQuery(queryMap: Map[String, String], partitions: Map[String, Array[String]], dataset: String, qName: String): (Long, Long, Long, Dataset[Row]) = {
    val (loadDataSize, loadTime) =  preloadTables(partitions, qName)

    if(firstRun == 0){
      val result = executeFinalQuery(queryMap)
      //result.foreach(r => println("RES: "+r))
      result.count
      //println(result.explain(true))
      firstRun = 1
    }
    val result = executeFinalQuery(queryMap)

    var t1 = System.nanoTime()
    result.count//.write.csv(Constants.HDFS + dataset + "/result_" + subPartitionMode +"/" + qName)
    var t2 = System.nanoTime()
    var duration = (t2 - t1) / 1000 / 1000

    //println(result.explain(true))

    unloadTables(partitions, qName);
    (duration, loadTime, loadDataSize, result)

  }

  def executeFinalQuery(queryMap: Map[String, String]): Dataset[Row] = {
    if(queryMap.size == 1){
      val query = queryMap.values.toSeq(0)
      spark.sqlContext.sql(query)

      //spark.sqlContext.sql(query)
    }
    else {
      queryMap.map(_._2).map(query => spark.sql(query)).reduceLeft((left, right) => (customJoin(left, right)))
    }

  }

  def customJoin(left: Dataset[Row], right: Dataset[Row]): Dataset[Row] = {
    val commonCols = commonColumns(left, right)

    if(commonCols.size > 0){
      val result = left.join(right, commonCols, "inner")
      result
    }
    else {
      val result = left.crossJoin(right)
      result
    }
  }

  def preloadTables(partitions:Map[String, Array[String]], queryName: String):(Long, Long) = {
    import spark.implicits._
    var loadDataSize: Long = 0
    var loadTime: Long = 0

    //Load levels
    if(selectedGraph == 1){ //Exact Query Answering
      //val cleanName = "TABLE_" + queryName.replace("-","_")

      partitions.foreach{case(partition) => { //partition._1 //predicate (http://example.org/gmark/keywords) -- partition._2: Array[lv1,lv3]
        var datasets = Seq.empty[(String,String)].toDS()
          .withColumnRenamed("_1", "s")
          .withColumnRenamed("_2", "o")

        var cleanName = partition._1 //predicate
        var levels = partition._2
        //println("predicate:" + cleanName)

        levels.foreach{case(level) => {// union the predicate table that exists to each level
          //println("partition: " + level + "/_3=_" + uriToStorage(cleanName) + "_")
          val dataset = loadDataset(level + "/_3=_" + uriToStorage(cleanName) + "_") //todo: why "_"
          datasets = datasets.union(dataset)
        }}

        datasets = datasets.repartition(30);

        //datasets = loadDataset("lv1")
        datasets.createOrReplaceTempView("table" +  uriToStorage(cleanName))
        spark.sqlContext.cacheTable("table" +  uriToStorage(cleanName))
        var start = System.currentTimeMillis;
        var size = datasets.count();
        var time = System.currentTimeMillis - start;
        println(cleanName+ ": \t\tCached "+size+" Elements in "+time+"ms");

        loadDataSize = loadDataSize + size
        loadTime = loadTime + time
      }}

    }else if (selectedGraph == 2){ //Approximate Query Answering
      //val cleanName = "TABLE_" + queryName.replace("-","_")

      partitions.foreach{case(partition) => { //partition._1 //predicate (http://example.org/gmark/keywords) -- partition._2: Array[lv1,lv3]
        var datasets = Seq.empty[(String,String)].toDS()
          .withColumnRenamed("_1", "s")
          .withColumnRenamed("_2", "o")

        var cleanName = partition._1 //predicate
        var levels = partition._2
        //println("predicate:" + cleanName)

        levels.foreach{case(level) => {// union the predicate table that exists to each level
          //println("partition: " + level + "/_3=_" + uriToStorage(cleanName) + "_")
          val dataset = loadDataset(level + "/_3=_" + uriToStorage(cleanName) + "_") //todo: why "_"
          datasets = datasets.union(dataset)
        }}

        datasets = datasets.repartition(30);

        //datasets = loadDataset("lv1")
        datasets.createOrReplaceTempView("table" +  uriToStorage(cleanName))
        spark.sqlContext.cacheTable("table" +  uriToStorage(cleanName))
        var start = System.currentTimeMillis;
        var size = datasets.count();
        var time = System.currentTimeMillis - start;
        println(cleanName+ ": \t\tCached "+size+" Elements in "+time+"ms");

        loadDataSize = loadDataSize + size
        loadTime = loadTime + time
      }}

    }
    /*else{ //Load original dataset with unions of levels.
      val cleanName = "TABLE_" + queryName.replace("-","_")

      var datasets = Seq.empty[(String,String,String)].toDS()
        .withColumnRenamed("_1", "s")
        .withColumnRenamed("_2", "p")
        .withColumnRenamed("_3", "o")

      var dataALL = List[Dataset[Row]]()
      //todo: change the way that concat the the datasets
      for(i <- 1 to 7){
        val dataset = loadDataset("lv"+i.toString)
        dataALL = dataALL :+ dataset
        println("partition: " + "lv"+i.toString)
        //datasets = dataset.union(datasets)
      }
      println("dataALL size: "+dataALL.size)
      val datasetsALL =  dataALL.reduce(_.union(_))

      datasetsALL.createOrReplaceTempView(cleanName)
      spark.sqlContext.cacheTable(cleanName)
      var start = System.currentTimeMillis;
      var size = datasetsALL.count();
      var time = System.currentTimeMillis - start;
      println(cleanName+ ": \t\tCached "+size+" Elements in "+time+"ms");

      loadDataSize = loadDataSize + size
      loadTime = loadTime + time

    }*/
 else{ //Load original datase
      val dataset = loadDataset(Constants.ORIGINAL_GARPH)
      val cleanName = "TABLE_" + queryName.replace("-","_")

     /* val data = dataset.as[(String, String, String)]
        .withColumnRenamed("_1", "s")
        .withColumnRenamed("_2", "p")
        .withColumnRenamed("_3", "o")
        .repartition(30);
*/
      dataset.createOrReplaceTempView(cleanName)
      spark.sqlContext.cacheTable(cleanName)

      var start = System.currentTimeMillis;
      var size = dataset.count();
      var time = System.currentTimeMillis - start;

      println(cleanName+ ": \t\tCached "+size+" Elements in "+time+"ms");

      loadDataSize = loadDataSize + size
      loadTime = loadTime + time

 }

 (loadDataSize, loadTime)
}

def loadDataset(file: String): Dataset[Row] = {
 import spark.implicits._
 //println("loadDataset:"+file)
 //load file pointed by index
 var input: String = ""
 if(selectedGraph == 1)
   input = hdfs + dataset + Constants.clusters + Constants.level + file + "/*"
 else
   input = hdfs + dataset + Constants.clusters + Constants.dataset + "/*"

 println("Input:" +  input)
 val table = spark.read.load(input)
   .as[(String, String)]
   .withColumnRenamed("_1", "s")
   .withColumnRenamed("_2", "o")
   //.repartition(10);
 // .withColumnRenamed("_2", "p")

 dataMap = dataMap + (file -> table)

 return  dataMap(file)
}



def unloadTables(partitions:Map[String, Array[String]], queryName:String): Unit = {
 partitions.foreach { case (partition) => {
   //val cleanName = "TABLE_" + queryName.replace("-","_")
   val cleanName = partition._1
   var start = System.currentTimeMillis;
   //todo: old method--> spark.sqlContext.dropTempTable(cleanName);
   spark.catalog.dropTempView("table" +  uriToStorage(cleanName))
   var time = System.currentTimeMillis - start;
   println(" Uncached  in " + time + "ms");
 }}
 //spark.sqlContext.clearCache()
 spark.catalog.clearCache()
}


/**
* returns true if an array contains only empty Strings
*/
def isEmptyArray(array: Array[String]): Boolean = {
 array.foreach(x => if(!x.isEmpty) return false)
 return true
}

/**
* tranforms map to mutable
*/
def toMutable[A, T](map: scala.collection.immutable.Map[A, T]) = {scala.collection.mutable.Map() ++ map}

/**
* project result on variables
*/
def projectResult(result: Dataset[Row], variables: Array[String]) = {
 if(isEmptyArray(variables))
   result
 else
   result.select(variables.head, variables.tail: _*)
}

def commonColumns(left: Dataset[Row], right: Dataset[Row]) = {
 left.columns.intersect(right.columns).toSeq
}

/**
* replaces prefix in triple patterns
*/
def replacePrefix(prefixMap: Map[String, String], triplePatternsStr: String) = {
 var tps = triplePatternsStr
 prefixMap.foreach{case(k, v) => {
   tps = tps.replaceAll(k + ":", v)
 }}
 tps
}

/**
* Calculates the  number of variables in a triple pattern
*/
def numberOfVars(triplePattern: (String, String, String)) = {
 triplePattern.productIterator.filter(_.toString.contains("?")).size
}

/**
* Finds variables in a triple patterm
*/
def findTPVars(tp: Tuple3[String, String, String]): Array[String] = {
 tp.productIterator.zipWithIndex
   .filter(_._1.toString.contains("?"))
   .map(x =>  QueryParser.removeNonAlphaNum(x._1.toString))
   .toArray
}



/**
* Returns list of files of the given folder
*/
def getListOfFiles(dir: String):List[File] = {
 val d = new File(dir)
 if (d.exists && d.isDirectory) {
   d.listFiles.filter(_.isFile).toList
 } else {
   List[File]()
 }
}

def listFiles(base: File, recursive: Boolean = true): Seq[File] = {
  val files = base.listFiles
  val result = files.filter(_.isFile)
  result ++
   files
     .filter(_.isDirectory)
     .filter(_ => recursive)
     .flatMap(listFiles(_, recursive))
}

}