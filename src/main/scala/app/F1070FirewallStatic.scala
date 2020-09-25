package app

import java.sql.DriverManager
import java.util.Properties

import dao.FirewallVisitFrequent
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import util.F1070LogParser

/**
 * #程序运行说明，首先创建数据库和数据表，然后用spark-submit提交任务
 * # 1.创建数据表
 create table f1070firewallstatic (
     id int(20) primary key auto_increment,
     ipandport varchar(100),
     count int(20)
 );
 *
 # 2.执行提交命令，该命令会下载程序所依赖的数据库驱动文件
 spark-submit --class app.F1070FirewallStatic --packages "mysql:mysql-connector-java:5.1.46" --master yarn --deploy-mode client DataAnalysis.jar  /tmp/netdevlog/f1070*
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

    //preProcessedRdd.take(20).foreach(println)
    //return

    val sourceIpRdd = preProcessedRdd.map(item=>item.split(" ")).map(item=>(item(0)+":"+item(1),1))
        .reduceByKey((a,b)=>a+b).map(item=>(item._2,item._1)).sortByKey(ascending = false)
        .take(num=500)
        .map(item =>FirewallVisitFrequent(item._2,item._1.toInt))

    val destinationIpRdd = preProcessedRdd.map(item=>item.split(" ")).map(item=>(item(2)+":"+item(3),1))
      .reduceByKey((a,b)=>a+b).map(item=>(item._2,item._1)).sortByKey(ascending = false)
      .take(num=500)
      .map(item =>FirewallVisitFrequent(item._2,item._1.toInt))

    //将数据转变为DataFrame的形式
    val sourceIpDf = spark.createDataFrame(sourceIpRdd)
    val destinationIPDf = spark.createDataFrame(destinationIpRdd)

    //targetIpRdd.foreach(println)

    clearTableInDatabase()

    //将数据分析的结果写入数据库,模式为"append"这样能确保自动生成主键
    val prop = new Properties();
    prop.put("user","root")
    prop.put("driver","com.mysql.jdbc.Driver")
    sourceIpDf.write.mode("append").jdbc("jdbc:mysql://172.16.5.42/work","work.f1070firewallstatic",prop)
    destinationIPDf.write.mode("append").jdbc("jdbc:mysql://172.16.5.42/work","work.f1070firewallstatic",prop)


    //关闭spark
    spark.stop()
  }

  def help(): Unit ={
    println("Use path as flowing!")
    println("file:///e:/info.txt  or  /tmp/netdevlog/f1070-2020.09")
  }

  def clearTableInDatabase(): Unit={
    val url = "jdbc:mysql://172.16.5.42:3306/work?useUnicode=true&characterEncoding=utf-8&useSSL=false"
    val driver = "com.mysql.jdbc.Driver"
    val username = "root"
    //var connection: Connection = _

    Class.forName(driver)
    //得到连接
    val connection = DriverManager.getConnection(url, username, "")
    val statement = connection.createStatement
    //删除所有内容
    val rs = statement.execute("DELETE FROM f1070firewallstatic")
  }
}
