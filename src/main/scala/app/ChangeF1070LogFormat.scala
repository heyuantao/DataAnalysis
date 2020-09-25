package app

import java.util.Properties

import app.F1070FirewallStatic.help
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import util.F1070LogParser

object ChangeF1070LogFormat {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.INFO)

    if (args.length != 2) {
      help()
      return
    }

    val inputFilePath = args(0)
    val spark = SparkSession.builder.appName("ChangeF1070LogFormat").getOrCreate()
    val lines = spark.read.textFile(inputFilePath).rdd

    val f1070LogParser = new F1070LogParser()

    val preProcessedRdd = lines.map(item =>f1070LogParser.parseLine(item)).filter(item=>item.isValid)
      .map(item=>item.toString)
      .map(_.split(" "))
      .map(item=>Visit(item(0),item(1).toInt,item(2),item(3).toInt,item(4),item(5)))

    //preProcessedRdd.saveAsTextFile(args(1))
    val preProcessedDf = spark.createDataFrame(preProcessedRdd)
    preProcessedDf.take(10).foreach(println)
    preProcessedDf.printSchema()

    //转变后将文件进行保存,格式可以为json或者parquet
    preProcessedDf.write.format("json").save(args(1))

    //关闭spark
    spark.stop()
  }

  def help(): Unit ={
    println("Must have input and output path !")
    println("Path format must be in file:///e:/info.txt  or  /tmp/netdevlog/f1070-2020.09 format !")
  }

  case class Visit(sourceip:String, sourceport:Int, destip:String, destport:Int, protocol:String, time:String)

}

