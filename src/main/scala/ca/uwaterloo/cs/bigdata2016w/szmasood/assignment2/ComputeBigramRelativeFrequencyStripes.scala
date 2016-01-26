package ca.uwaterloo.cs.bigdata2016w.szmasood.assignment2;

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer


object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)


    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Bigram Count")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    val counts = textFile.flatMap(line => {
        val tokens = tokenize(line)
        val stripes = new scala.collection.mutable.HashMap[String, scala.collection.mutable.HashMap [String,Float]]()
        var prev = ""
        var curr = ""
        if (tokens.length > 1) {
          for (i <- 1 until tokens.length) {
            prev = tokens(i - 1)
            curr = tokens(i)

            val stripe = stripes.getOrElse(prev, HashMap[String, Float]())
            stripe.put(curr, stripe.getOrElse(curr, 0f) + 1f)
            stripes.put(prev, stripe)
          }
          stripes.toIterator
        }
        else {
          Iterator()
        }
      })
      .reduceByKey((a,b)=> {
          for (k <- b.keySet){
            val sum = a.getOrElse(k, 0.0f) + b(k)
            a.put (k, sum)
          }
          a
        })
        .map (r => {
          val stripe = scala.collection.mutable.HashMap [String,Float] ()
          var sum = 0f
          for (p <- r._2.keySet) {
            sum += r._2(p)
          }
          for (s <- r._2.keySet) {
            stripe += s -> (r._2(s)/sum)
          }
          s"${r._1}\t${stripe}"
        })

    counts.saveAsTextFile(args.output())
  }
}
