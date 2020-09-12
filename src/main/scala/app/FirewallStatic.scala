package app

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import util.F1070LogParser

object FirewallStatic {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.INFO)

    if (args.length == 0) {
      help()
      return
    }

    val inputFilePath = args(0)
    val spark = SparkSession.builder.appName("CountVisited").getOrCreate()
    val lines = spark.read.textFile(inputFilePath).rdd

    val f1070LogParser = new F1070LogParser()

    val preProcessedRdd = lines.map(item =>f1070LogParser.parseLine(item)).filter(item=>item.isValid)
      .map(item=>item.toString)

    val targetIpRdd = preProcessedRdd.map(item=>item.split(" ")(3)).map(item=>(item,1))
        .reduceByKey((a,b)=>a+b).map(item=>(item._2,item._1)).sortByKey(ascending = false).take(50)

    targetIpRdd.foreach(println)

  }

  def help(): Unit ={
    println("Use path as flowing!")
    println("file:///e:/info.txt  or  /tmp/netdevlog/f1070-2020.09")
  }

}
