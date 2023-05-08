package org.eswc.smarter

import org.apache.spark.sql.SparkSession


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

object QueryTranslator {

  val spark = loadSparkSession()
  var hdfs: String = ""
  var vpProperties =  Map[String, String]()
  //var instanceOBJIndex = Map[String, Array[Int]]



  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val sc = spark.sparkContext

    if (args.size != 6) {
      println("Missing Arguments")
      println("Arguments: number of partitions, dataset name, balance, hdfs base path, sparql query path")
      System.exit(-1)
    }

    val dataset = args(0)
    hdfs = args(1)

    if (!hdfs.endsWith("/"))
      hdfs = hdfs + "/"

    var inputPath = args(2)
    var propertyIndexPath = args(3)

    // Map[String, Array[String]] -- key: properties, value: levels that properties exist
    val propertyIndex = loadPropertyIndex(propertyIndexPath)

    var instanceSUBIndexPath = args(4)
    var instanceOBJIndexPath = args(5)
    val instanceSUBIndex = loadInstanceSUBIndex(instanceSUBIndexPath.trim) // !!!!Map[String, String]() //!!!loadInstanceSUBIndex(instanceSUBIndexPath.trim)
    val instanceOBJIndex = loadInstanceOBJIndex(instanceOBJIndexPath.trim)  //!!Map[String, Array[Int]]() //!!!

    //propertyIndex.foreach {keyVal => println(keyVal._1 + "=" + keyVal._2)}

    val fileList = listFiles(new File(inputPath), true)

    fileList.foreach(queryFile => {

      vpProperties = Map[String, String]()

      val queryPath = queryFile.getPath
      val queryName = queryFile.getName

      println("queryName: "+ queryName)
      println("queryName.dropRight(4): "+ queryName.dropRight(4))

      //create translated queries folder
      val translateFolder = inputPath + "/" + "translated_queries" + "_" + dataset

      if(!new File(translateFolder).exists) {
        val mkdir = "mkdir " + translateFolder !
      }
      val fullPath = translateFolder + "/"// + subDir + "/"
      if(!new File(fullPath).exists) {
        val mkdir = "mkdir " + fullPath !
      }

      //Parse query
      QueryParser.parseQuery(queryFile)

      //val variables = QueryParser.variables //Array[String]
      val triplePatterns = QueryParser.triplePatterns //Array[(Int, (String, String, String))]
      //val levels = QueryParser.levels


      triplePatterns.foreach(t => println(t._1  +  ": "+ t._2 ))
      //levels.foreach(println(_))

      val pw = new PrintWriter(new File(fullPath + queryName))

      val aggrTps = aggregateTps(triplePatterns) // [(x1,(id1,id2,..),  (x2, (id2,id4)),..] -- x:variable, id:triple pattern
      println("queryName: "+queryName)
      //val queryMap = translateTp(aggrTps, toMutable(triplePatterns.toMap), queryName.dropRight(4)) //swdf: query.txt
      //val queryMap = translateTp(aggrTps, toMutable(triplePatterns.toMap), queryName.dropRight(7)) //shop: query.sparql
      //val queryMap = translateTp(aggrTps, toMutable(triplePatterns.toMap), queryName, propertyIndex) //shop: query
      val queryMap = translateTp(aggrTps, toMutable(triplePatterns.toMap), queryName.dropRight(4), propertyIndex, instanceSUBIndex, instanceOBJIndex) //shop: query.txt
      //println("---> "+ queryMap.size)
      pw.write(">>>>> " + queryName + "\n")
      queryMap.foreach{case(k, v) => {
        println("\n"+k+ "  *******  :"+v)
        pw.write(k + " " + v + "\n")
      }}

      //todo: change vpProperties
      //val partitions = queryIndex.values.toArray.distinct.mkString(",")
      //val partitions = levels.map(x => x.trim()).mkString(",") //queryIndex.values.reduce((x, y) => x ++ y).distinct.mkString(",")
       vpProperties.foreach{vp => pw.write("partitions " + vp._1 + " " + vp._2 + "\n")}
        //!pw.write("partitions " + partitions + "\n")
        pw.write("TP " + triplePatterns.size)
        pw.close

    })
  }

  //Load Index about the levels in which each predicate exists
  def loadPropertyIndex(propertyIndexPath: String): Map[String, Array[Int]] = {
    var propertyIndex = Map[String, Array[Int]]()
    for (line <- Source.fromFile(propertyIndexPath).getLines) {
      val tokens: Array[String] = line.split(" ")
      val property: String = tokens(0).trim

      //val rangeLev: Array[Int] = tokens.drop(1).mkString.split(",").map(_.toInt)
      val levels = tokens(1).split(",").map(_.toInt)//(rangeLev(1) to rangeLev(0)).toArray
      //println(property)
      //println("  "+levels.mkString("*"))
      propertyIndex = propertyIndex + (property -> levels)
    }
    propertyIndex
  }

  def loadInstanceOBJIndex(propertyIndexPath: String): Map[String, Array[Int]] = {
    var propertyIndex = Map[String, Array[Int]]()
    for (line <- Source.fromFile(propertyIndexPath).getLines) {
      val tokens: Array[String] = line.split(" ")
      //println("line:"+line)
      //println("tok.size: "+tokens.size)
      if(tokens.size > 1){
        val instance: String = tokens(0).trim
        //println("OBJ Ins:"+instance)
        //val rangeLev: Array[Int] = tokens.drop(1).mkString.split(",").map(_.toInt)
        val levels = tokens(1).split(",").map(_.toInt)//(rangeLev(1) to rangeLev(0)).toArray
        //println(property)
        //println("  "+levels.mkString("*"))
        propertyIndex = propertyIndex + (instance -> levels)

      }

    }
    propertyIndex
  }

  def loadInstanceSUBIndex(instanceIndexPath: String): Map[String, String] = {
    var instanceIndex = Map[String, String]()
    val fileInstList = listFiles(new File(instanceIndexPath), true)
    fileInstList.foreach(levelFile => { //file containing instances of a specific level

      val instancePath = levelFile.getPath
      val level = levelFile.getName.takeRight(1)
      println("Files SUB Instance: "+levelFile.getName + "- level: " + level)
      //val instances = Source.fromFile(instancePath).getLines.mkString
      for (line <- Source.fromFile(instancePath).getLines) {
        instanceIndex = instanceIndex + (line -> level)
      }
    })
    instanceIndex
  }


  /**
   * Initializes spark session
   */
  def loadSparkSession() = {
    val spark = SparkSession.builder
      .appName("QueryTranslatorVP2")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.speculation", "true")
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    spark
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

  //Return  [(x1,(id1,id2,..),  (x2, (id2,id4)),..] -- x:variable, id:triple pattern
  def aggregateTps(triplePatterns: Array[(Int, (String, String, String))]): Map[String, Array[Int]] = {
    val aggrTps = triplePatterns//.map{case(id, tp) => (findTPVars(tp)(0), Array(id))}.toMap
      .flatMap{case(id, tp) => findTPVars(tp).map(x => (x, id))} //(variables, idTriple)
      .groupBy(_._1) //Map(x, List(id1, id2,..))
      .map(x => (x._1, x._2.map(_._2)))// [(x1,(id1,id2,..),  (x2, (id2,id4)),..] -- x:variable
    return toMutable(aggrTps)
  }

  /**
   * Translate aggregated triple patterns into SQL.
   */
  //aggrTps: [x1,Array(tpId1,tpId2)] -- triplePatterns: [tpId1, (s,p,o)] -- queryIndex: [tpId1, partition]
  def  translateTp(aggrTps: Map[String, Array[Int]], triplePatterns: Map[Int, (String, String, String)], queryName:String, propertyIndex:Map[String, Array[Int]], instanceSUBIndex:Map[String, String], instanceOBJIndex:Map[String, Array[Int]]): Map[String, String] = {
    var usedIds = Array[Int]()
    var queryMap = Map[String, String]()
    val sortedTps = ListMap(aggrTps.map(x => (x, x._2.size)).toSeq.sortWith(_._2 > _._2):_*).map(_._1) //[(variable, (id1, id2)),..] -- ListMap maintains the order of elemnents --First variables contained in more triple.

    var i = 0
    var selectVariables = Set[String]()
    var joinTables = Map[String, Array[String]]() //[variable, (tab0, tab2)]
    var subQueries = Array[String]()
    var varTableMap = Map[String, String]()
    sortedTps.foreach{case(variable, ids) => { // order based on the number of triples that the variable is participated
      //remove for removing query reordering
      //!!val sortedIds = ListMap(ids.map(id => (id, findTPVars(triplePatterns(id)).size)).sortWith(_._2 > _._2):_*)//.map(_._1)

      ids.foreach{case(id) => { //id: triple pattern
        val tp = triplePatterns(id)//(s,p,o)
        val tableName = "tab" + id //tab0: triple pattern "0"
        //not VP tables
        //val subQuery = translate("TABLE_" + queryName.replace("-","_") , tp) + " AS " + tableName  //(SELECT ...WHERE partition)AS tab1
        //VP tables
        val subQuery = translate(propertyIndex, instanceSUBIndex, instanceOBJIndex, tp) + " AS " + tableName  //(SELECT ...WHERE partition)AS tab1

        if(joinTables.contains(variable))  //[variable, (tab0, tab2)]
            joinTables = joinTables + (variable -> (joinTables(variable) ++ Array(tableName)))
        else
            joinTables = joinTables + (variable -> Array(tableName))

        if(!usedIds.contains(id)) {
            varTableMap = varTableMap ++ extractVarTableMappings(subQuery, tableName)
            //varTableMap.foreach(v => println("<<  "+v))
            selectVariables = selectVariables ++ findTPVars(tp)
            subQueries = subQueries :+ subQuery
            usedIds = usedIds :+ id
        }
      }}
      i+=1
    }}
    var finalQuery = ""
    var joinCondition = " WHERE "
    //val rootTable = joinTables(0) + "." + variable
    finalQuery+= "SELECT " + selectVariables.map(v => varTableMap(v) + "." + v + " AS " + v)   //tab0.type AS type
      .mkString(",") + " FROM "
    subQueries/*.reverse*/.foreach(q => {  //remove for removing query reordering
      finalQuery = finalQuery + q + ", "
    })
    //subQueries.foreach(q => println(q))

    finalQuery = finalQuery.patch(finalQuery.lastIndexOf(","), "", 2)
    //println("1 - finalQuery:  "+finalQuery)

    joinTables.foreach{case(v, tables) => { //[variable, (tab0, tab2)]
      for(i <- 0 to tables.length){
        if(i+1 < tables.length){
          joinCondition = joinCondition + tables(i) + "." + v + "=" + tables(i+1) + "." + v
          joinCondition = joinCondition + " AND "
        }
      }
    }}

    joinCondition = joinCondition.patch(joinCondition.lastIndexOf(" AND"), "", 4)

    if(joinCondition.size > 7)
      finalQuery+= joinCondition

    //println("2 - finalQuery:  "+finalQuery)

    return queryMap + ("X" -> finalQuery)
  }

  def extractVarTableMappings(query: String, table: String): Map[String, String] = {
    val vars = query.substring(query.indexOf("SELECT") + 6, query.indexOf("FROM"))
    var varTableMap = Map[String, String]()
    vars.split(",").foreach(v => {
      val cleanV = v.substring(v.indexOf("AS")+2).trim
      varTableMap = varTableMap + (cleanV -> table)
    })

    //varTableMap.foreach(x => println(x))
    return varTableMap
  }

  // partition: table00000__3_E__http___purl_org_dc_elements_1_1_creator_   -- tp: (s,p,o)
  def translate(propertyIndex:Map[String, Array[Int]], instanceSUBIndex:Map[String, String], instanceOBJIndex:Map[String, Array[Int]], tp: (String, String, String)): String = {
    import spark.implicits._
    numberOfVars(tp) match {
      case 1 => singleVarTPSql(propertyIndex, instanceSUBIndex, instanceOBJIndex, tp)
      case 2 => doubleVarTPSql(propertyIndex, instanceSUBIndex, instanceOBJIndex, tp)
      case _ => threeVarTPSql(propertyIndex, instanceSUBIndex, instanceOBJIndex, tp)
    }
  }


  //partition: TABLE_
  def singleVarTPSql(propertyIndex:Map[String, Array[Int]], instanceSUBIndex:Map[String, String],  instanceOBJIndex:Map[String, Array[Int]], tp: Tuple3[String, String, String]): String = {
    val (s, p, o) = tp
    var query: String = null
    if(s.contains("?")){
      val v = QueryParser.removeNonAlphaNum(s)
      //query = "(SELECT s AS " + v + " FROM " + partition + " WHERE p == '" + p + "' AND o == '" + o + "')"  //not VP tables
      //val table = p.replace("-", "_").trim //.replace("=", "_E_")
      // + "-" + "_3=" + uriToStorage(sp)
      var pred = p.replace("<", "").replace(">", "").trim

      val predLevels = propertyIndex(pred)
      val objLevels = instanceOBJIndex(o.trim)
      val levels = objLevels.intersect(predLevels)

      //val levelsPred = propertyIndex(pred).mkString(",") //levels based on predicate
      //val levelsObj = instanceOBJIndex(o.trim).mkString(",")
      //println("singleVarTPSql subj: "+ levelsPred + " -- " + levelsObj)

      vpProperties = vpProperties + (pred -> levels.mkString(","))
      /*if (levelsObj.length < levelsPred.length){
        vpProperties = vpProperties + (pred -> levelsObj)
      }else{
        vpProperties = vpProperties + (pred -> levelsPred)
      }*/

      val table = uriToStorage(pred) //.replace("/", "_").replace("=", "_E_").trim
      query = "(SELECT s AS " + v + " FROM " + "table" + table + " WHERE o == '" + o + "')"
    }
    else if(p.contains("?")){ //todo: search all the vp tables on the level
      val v = QueryParser.removeNonAlphaNum(p)
      val level = instanceSUBIndex(s.replace("<", "").replace(">", "").trim)
      vpProperties = vpProperties + ("table" -> level)
      //todo: chnage the name of the table
      query = "(SELECT p AS " + v + " FROM " + "table" + " WHERE " + "s == '" + s + "' AND o == '" + o + "')"

    }
    else{
      val v = QueryParser.removeNonAlphaNum(o)
      var pred = p.replace("<", "").replace(">", "").trim
      //val levelsPred = propertyIndex(pred).mkString(",") //levels based on predicate
      val levelSUB = instanceSUBIndex(s.replace("<", "").replace(">", "").trim).mkString(",")
      vpProperties = vpProperties + (pred -> levelSUB)

      val table = uriToStorage(pred)//.replace("/", "_").replace("=", "_E_").trim
      query = "(SELECT o AS " + v + " FROM " + "table" + table + " WHERE s == '" + s + "')"
    }
    return query
  }

  def doubleVarTPSql(propertyIndex:Map[String, Array[Int]], instanceSUBIndex:Map[String, String],  instanceOBJIndex:Map[String, Array[Int]], tp: Tuple3[String, String, String]): String = {
    val (s, p, o) = tp
    var query: String = null

    if(p.contains("?") && s.contains("?") ) {
      val v1 = QueryParser.removeNonAlphaNum(s)
      val v2 = QueryParser.removeNonAlphaNum(p)
      val levels = instanceOBJIndex(o.trim).mkString(",")
      vpProperties = vpProperties + ("table" -> levels)
      //todo: chnage the name of the table
      query = "(SELECT s AS " + v1 +  ", p AS " + v2 + " FROM " + "table" + " WHERE o == '" + o + "')"
    }
    else if(p.contains("?") && o.contains("?")) {
      val v1 = QueryParser.removeNonAlphaNum(p)
      val v2 = QueryParser.removeNonAlphaNum(o)
      val level = instanceSUBIndex(s.replace("<", "").replace(">", "").trim)
      vpProperties = vpProperties + ("table" -> level)
      //todo: chnage the name of the table
      query = "(SELECT p AS " + v1 + ", o AS " + v2 + " FROM " + "table" + " WHERE s == '" + s + "')"
    }
    else {
      val v1 = QueryParser.removeNonAlphaNum(s)
      val v2 = QueryParser.removeNonAlphaNum(o)
      var pred = p.replace("<", "").replace(">", "").trim
      val levels = propertyIndex(pred).mkString(",")
      val table = uriToStorage(pred)//.replace("/", "_").replace("=", "_E_").trim
      vpProperties = vpProperties + (pred -> levels)

      query = "(SELECT s AS " + v1 + ", o AS " + v2 + " FROM " + "table" + table + ")"
    }
    return query
  }

  def threeVarTPSql(propertyIndex:Map[String, Array[Int]], instanceIndex:Map[String, String],  instanceOBJIndex:Map[String, Array[Int]], tp: Tuple3[String, String, String]): String = {
    val (s, p, o) = tp
    val v1 = QueryParser.removeNonAlphaNum(s)
    val v2 = QueryParser.removeNonAlphaNum(p)
    val v3 = QueryParser.removeNonAlphaNum(o)
    val query = "(SELECT s AS " + v1 + ", p AS " + v2 + ", o AS " + v3 + " FROM " + "TABLE_" + ")"
    return query
  }


  /**
   * tranforms map to mutable
   */
  def toMutable[A, T](map: scala.collection.immutable.Map[A, T]) = {scala.collection.mutable.Map() ++ map}

  def uriToStorage(uri: String) = {
    uri.replaceAll("[^A-Za-z0-9]", "_").toLowerCase
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
