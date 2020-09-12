package app

import org.apache.spark.sql.SparkSession
import util.{F1070LogParser}

object CountVisit {
  def main(args: Array[String]) {

    if (args.length == 0) {
      help()
      return
    }

    val inputFilePath = args(0)
    val spark = SparkSession.builder.appName("lineCount").getOrCreate()
    val lines = spark.read.textFile(inputFilePath).rdd

    val f1070LogParser = new F1070LogParser()

    val outputRdd = lines.map(item =>f1070LogParser.parseLine(item)).filter(item=>item.isValid)
      .map(item=>item.getSourceIp)

    val first = outputRdd.take(10)
    first.foreach(println)
  }
  def help(): Unit ={
    println("Use path as flowing!")
    println("file:///e:/info.txt  or  /tmp/netdevlog/f1070-2020.09")
  }
}
