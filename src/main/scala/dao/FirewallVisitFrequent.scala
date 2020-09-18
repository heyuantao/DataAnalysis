package dao

/**
 * 用来描述对某个IP:PORT的访问量，count表示访问的次数
 */
case class FirewallVisitFrequent(ipandport:String,count:Int)
