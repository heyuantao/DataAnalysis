package app

import org.apache.spark.sql.SparkSession

object LineCount {
  def main(args: Array[String]) {
    if(args.length==0){
      help()
      return
    }
    val inputFilePath = args(0)
    val spark = SparkSession.builder.appName("lineCount").getOrCreate()
    val file = spark.read.textFile(inputFilePath)
    val count = file.count()
    println("Number of line is:"+count)
    spark.stop()
  }


  def help(): Unit ={
    println("Use path as flowing!")
    println("file:///e:/info.txt  or      ")
  }
}
