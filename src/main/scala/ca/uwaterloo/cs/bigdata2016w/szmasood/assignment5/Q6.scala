package ca.uwaterloo.cs.bigdata2016w.szmasood.assignment5;

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable.ListBuffer



object Q6  {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.date())

    val dt = args.date()


    val conf = new SparkConf().setAppName("q6")
    val sc = new SparkContext(conf)

    val base = args.input()
    val lineItemUri = s"${base}/lineitem.tbl"

    val lineOrderKeys = sc.textFile(lineItemUri)
      .filter (r => r.split("\\|",12)(10).contains(dt))
      .map (r => {
        val sp = r.split("\\|",11)
        val l_returnflag = sp(8)
        val l_linestatus = sp(9)
        val l_quantity = sp(4)
        val l_extended_price = sp(5).toDouble
        val l_discount = sp(6).toDouble
        val l_tax = sp(7).toDouble

        val disc_price = l_extended_price*(1d-l_discount)
        val charge = disc_price*(1d+l_tax)

        Tuple2(Tuple2(l_returnflag, l_linestatus),Tuple6(l_quantity.toDouble, l_extended_price.toDouble, disc_price, charge,l_discount.toDouble, 1d))
      })
      .aggregateByKey((0d,0d,0d,0d,0d,0d))(
        (acc, value) => (acc._1 + value._1, acc._2 + value._2, acc._3 + value._3, acc._4 + value._4, acc._5 + value._5, acc._6 + 1d),
        (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2, acc1._3 + acc2._3, acc1._4 + acc2._4, acc1._5 + acc2._5, acc1._6 + acc2._6))
        .map (r => {
          val ret_flag = r._1._1
          val line_status = r._1._2
          val sum_quant = r._2._1
          val sum_base_price = r._2._2
          val sum_disc_price = r._2._3
          val sum_charge = r._2._4
          val sum_disc = r._2._5
          val count_order = r._2._6
          val avg_qty = sum_quant/count_order
          val avg_price = sum_base_price/count_order
          val avg_disc = sum_disc/count_order

          (ret_flag, line_status, sum_quant, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_price, avg_disc, count_order)
        })

    lineOrderKeys.collect.foreach(println)
  }

}
