package org.ics.isl

import scala.io.Source
import java.io.File
import scala.collection.mutable.Map
import org.apache.spark.sql._


object QueryParser {
  var file: File = null
  var query: String = null
  //var levels: Array[String] = null //extarct levels from query (initial)
  var variables: Array[String] = null
  var triplePatterns: Array[(Int, (String, String, String))] = null
  var isIndexedQuery: Boolean = false
  var queryMappings: Map[Int, String] = null


  def parseQuery(queryFile: File) = {
    file = queryFile
    query = Source.fromFile(queryFile).getLines.mkString
    //variables = extractSelectVariables() // Array[String]
    triplePatterns = extractTriplePatterns().zipWithIndex.map(x => (x._2, x._1))
    //levels = extractLevels()
  }

  def arrayEquals[A, B](a1: Array[A], a2: Array[B]): Boolean = {a1.diff(a2).isEmpty && a2.diff(a1).isEmpty}

  def extractQueryVars(): Array[String] = {
    triplePatterns.flatMap{case(id, tp) => findTPVars(tp)}.distinct
  }


  /**
   * Extract levels-graphs from query
   */
  def extractLevels(): Array[String] = {
    val levelsParts = "((?=from|FROM).*?(?=where|WHERE))".r
    val levels = levelsParts.findFirstMatchIn(query).mkString
    println("levels: "+levels)
    levels.split("FROM NAMED").drop(1)
            .map(x => x.replace(">", "").replace("<", ""))
            //.map(x => x.split("_")(1)) //SHOP dataset
            .map(x => x.split("//")(1))//SWDF dataset
  }

  /**
   * Extract select variables from query
   */
  def extractSelectVariables(): Array[String] = {
    val selectPattern = "\\b(SELECT|select.*?(?=from|FROM))\\b".r
    val selectVariables = selectPattern.findFirstMatchIn(query).mkString
    println("selectVariables: "+selectVariables)
    //drop SELECT replace non-alphanumeric
    selectVariables.split(" ").drop(1).map(removeNonAlphaNum(_))
  }

  /**
   * Extract triple pattern from query
   */
  def extractTriplePatterns(): Array[(String, String, String)] = {
    val pattern = "(\\{.*?\\})".r
    //extract string of triple patterns
    val triplePatternStr = pattern.findAllMatchIn(query)
      .mkString
      .drop(1)
      .dropRight(1)

    if(triplePatternStr.isEmpty){
      isIndexedQuery = false
      return Array()
    }
    else{
      //extract triple patterns from query
      var triplePatterns = triplePatternStr.split(" \\.")
                                            .filter(_.length > 10)
                                            .filter(!_.startsWith("#"))
                                            .map(x => cleanTp(x.trim.split("\\s+").map(_.trim)))
                                            .map(x => {
                                                    if(x(1) == "a" || x(1) == "rdf:type")
                                                        (x(0), Constants.RDF_TYPE, x(2))
                                                    else
                                                        (x(0), x(1), x(2))
                                            })
      return triplePatterns
    }
  }


  def isVariable(str: String): Boolean = str.contains("?")

  def isLiteral(str: String): Boolean = str.contains("\"")

  //Helper Methods

  /**
   * cleans triple pattern uris from < >
   */
  def cleanTp(tp: Array[String]): Array[String] = {
    tp.map(t => {
      if(t.contains("\"") || isVariable(t)) {
        t
      }
      else{
        if(!t.startsWith("<") && !t.endsWith("<")){
          "<" + t + ">"
        }
        else if(!t.startsWith("<")) {
          "<" + t
        }
        else if(!t.endsWith(">")) {
          t + ">"
        }
        else {
          t
        }
      }
    })
  }

  /**
   * removes every non alphanumeric char in string
   */
  def removeNonAlphaNum(str: String): String = {
    str.replaceAll("[^a-zA-Z0-9]", "")
  }

  /**
   * tranforms map to mutable
   */
  def toMutable[A, B](map: scala.collection.immutable.Map[A, B]) = {scala.collection.mutable.Map() ++ map}

  /**
   * Finds variables in a triple patterm
   */
  def findTPVars(tp: Tuple3[String, String, String]): Array[String] = {
    tp.productIterator.zipWithIndex
      .filter(_._1.toString.contains("?"))
      .map(x =>  removeNonAlphaNum(x._1.toString))
      .toArray
  }

}
