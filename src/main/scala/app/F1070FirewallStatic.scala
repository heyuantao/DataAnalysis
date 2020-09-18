package app

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import util.F1070LogParser

/**
 * #程序运行说明，首先创建数据库和数据表，然后用spark-submit提交任务
 * # 1.创建数据表
 * create table f1070firewallstatic (
 *    id int(20) primary key auto_increment,
 *    destinationip varchar(500),
 *    destinationport int(20),
 *    count int(20)
 * );
 *
 * # 2.执行提交命令，该命令会下载程序所依赖的数据库驱动文件
 * spark-submit --class app.F1070FirewallStatic --packages "mysql:mysql-connector-java:5.1.46"   --master yarn --deploy-mode client DataAnalysis.jar  /tmp/netdevlog/nginx*
 */

object F1070FirewallStatic {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.INFO)

    if (args.length == 0) {
      help()
      return
    }

    val inputFilePath = args(0)
    val spark = SparkSession.builder.appName("F1070FirewallStatic").getOrCreate()
    val lines = spark.read.textFile(inputFilePath).rdd

    val f1070LogParser = new F1070LogParser()

    val preProcessedRdd = lines.map(item =>f1070LogParser.parseLine(item)).filter(item=>item.isValid)
      .map(item=>item.toString)

    val targetIpRdd = preProcessedRdd.map(item=>item.split(" ")).map(item=>(item(0)+":"+item(1),1))
        .reduceByKey((a,b)=>a+b).map(item=>(item._2,item._1)).sortByKey(ascending = false)

    targetIpRdd.take(50).foreach(println)

  }

  def help(): Unit ={
    println("Use path as flowing!")
    println("file:///e:/info.txt  or  /tmp/netdevlog/f1070-2020.09")
  }

}
