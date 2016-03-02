package ca.uwaterloo.cs.bigdata2016w.szmasood.assignment5;

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.Date
import java.text.SimpleDateFormat
import scala.collection.mutable.ListBuffer



object Q7  {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.date())

    val dt = args.date()

    val conf = new SparkConf().setAppName("q7")
    val sc = new SparkContext(conf)

    val base = args.input()
    val lineItemUri = s"${base}/lineitem.tbl"

    val customerUri = s"${base}/customer.tbl"
    val ordersUri = s"${base}/orders.tbl"

    val customer = sc.broadcast(sc.textFile(customerUri).map(r => {
      val sp = r.split ("\\|",3)
      (sp(0), sp(1)) // (custKey, custName)
    }).collect.toMap)

    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val in_dt = sdf.parse(dt)

    val orders = sc.textFile(ordersUri)
      .filter (r => {
        val o_orderdate = sdf.parse(r.split("\\|",-1)(4))
        o_orderdate.before(in_dt)
      })
      .flatMap (r => {
        var res = ListBuffer [(String,(String,String,String))]()
        val sp = r.split("\\|",-1)
        if (customer.value.contains(sp(1))) {
          val cust_name = customer.value(sp(1))
          res += ((sp(0), (cust_name, sp(4), sp(7)))) // (orderkey, (cust_name, o_orderdate, o_ship_pri)
        }
        res
      })
      .cache()


    val lineOrderKeys = sc.textFile(lineItemUri)
      .filter (r =>  {
        val l_shipdate = sdf.parse(r.split("\\|",12)(10))
        l_shipdate.after(in_dt)
      })
      .map (r => {
        val sp = r.split("\\|",8)
        val l_orderkey = sp(0)
        val rev = sp(5).toDouble*(1d - sp(6).toDouble)
        (l_orderkey, rev)
      })
      .cogroup(orders)
      .filter(r=> r._2._1.size != 0 && r._2._2.size !=0)
      .flatMap (r=> {
        var resu = ListBuffer [((String,String,String,String),Double)]()

        val cust_name = r._2._2.head._1
        val l_orderkey = r._1
        val o_orderdate = r._2._2.head._2
        val o_shippro = r._2._2.head._3

        for (a <- r._2._1) {
          resu += (((cust_name,l_orderkey,o_orderdate,o_shippro),a))
        }

        resu
      })
      .reduceByKey((a,b)=> a + b)
      .map (r=> (r._1._1, r._1._2, r._2,r._1._3, r._1._4))

    lineOrderKeys.takeOrdered(10)(Ordering[Double].reverse.on(x=>x._3)).foreach(println)
  }

}
