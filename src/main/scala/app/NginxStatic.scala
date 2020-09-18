package app

import java.sql.{Connection, DriverManager}
import java.util.Properties

import dao.WebVisitFrequent
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import util.NginxLogParser

/**
 * #程序运行说明，首先创建数据库和数据表，然后用spark-submit提交任务
 * # 1.创建数据表
 * create table nginxstatic (id int(20) primary key auto_increment, url varchar(500), count int(20));
 *
 # 2.执行提交命令，该命令会下载程序所依赖的数据库驱动文件
 spark-submit --class app.NginxStatic --packages "mysql:mysql-connector-java:5.1.46"  --master yarn --deploy-mode cluster DataAnalysis.jar  /tmp/netdevlog/nginx*
 */

object NginxStatic {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.INFO)

    if (args.length == 0) {
      help()
      return
    }
    clearTableInDatabase()

    val inputFilePath = args(0)
    val spark = SparkSession.builder.appName("NginxStatic").getOrCreate()
    val lines = spark.read.textFile(inputFilePath).rdd

    val nginxLogParser = new NginxLogParser()

    val preProcessedRdd = lines.map(item =>nginxLogParser.parseLine(item)).filter(item=>item.isValid)
      .map(item=>item.toString)

    /**
     * 根据访问量对url进行排序，并最终转换为case class的形式
     */
    val targetIpRdd = preProcessedRdd.map(item=>item.split(" ")(1)).map(item=>(item,1))
      .reduceByKey((a,b)=>a+b).map(item=>(item._2,item._1)).sortByKey(ascending = false)
      .take(500) //取前200个
      .map(item =>WebVisitFrequent(item._2,item._1.toInt))

    //将数据转变为DataFrame的形式
    val targetIpDf = spark.createDataFrame(targetIpRdd)

    //targetIpRdd.take(100).foreach(println)

    //将数据分析的结果写入数据库,模式为"append"这样能确保自动生成主键
    val prop = new Properties();
    prop.put("user","root")
    prop.put("driver","com.mysql.jdbc.Driver")
    targetIpDf.write.mode("append").jdbc("jdbc:mysql://172.16.5.42/work","work.nginxstatic",prop)

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
    val rs = statement.execute("DELETE FROM nginxstatic")
  }
}

