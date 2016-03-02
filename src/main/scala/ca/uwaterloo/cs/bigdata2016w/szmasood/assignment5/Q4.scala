package ca.uwaterloo.cs.bigdata2016w.szmasood.assignment5;

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable.ListBuffer



object Q4  {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.date())

    val dt = args.date()


    val conf = new SparkConf().setAppName("q4")
    val sc = new SparkContext(conf)

    val base = args.input()
    val lineItemUri = s"${base}/lineitem.tbl"
    val ordersUri = s"${base}/orders.tbl"

    val customerUri = s"${base}/customer.tbl"
    val nationUri = s"${base}/nation.tbl"

    val customers = sc.broadcast(sc.textFile(customerUri).map(r => {
      val sp = r.split ("\\|",5)
      (sp(0), sp(3))
      }).collect.toMap)

    val nations = sc.broadcast(sc.textFile(nationUri).map(r => {
      val sp = r.split ("\\|",3)
      (sp(0), sp(1))
    }).collect.toMap)

    val orders = sc.textFile(ordersUri)
      .map (r => {
        val sp = r.split("\\|",3)
        (sp(0), sp(1))
      })
      .cache()

    val lineOrderKeys = sc.textFile(lineItemUri)
      .filter (r => r.split("\\|",12)(10).contains(dt))
      .map (r => {
        val sp = r.split("\\|",2)
        val l_order_key = sp(0)
        (l_order_key, 1)
      })
      .cogroup(orders)
      .filter(r=> r._2._1.size != 0)
      .flatMap(r => {
        var res = ListBuffer [((Int,String),Int)]()
        if (customers.value.contains(r._2._2.head)) {
          val nation_key = customers.value(r._2._2.head)
          val nation = nations.value(nation_key)
          for (s <- r._2._1) {
            res += (((nation_key.toInt,nation), 1))
          }
        }
        res
      })
      .reduceByKey(_+_)
      .map (r=> (r._1._1,r._1._2,r._2))
      .sortBy(r=> r._1)

    lineOrderKeys.collect.foreach(println)
  }

}
