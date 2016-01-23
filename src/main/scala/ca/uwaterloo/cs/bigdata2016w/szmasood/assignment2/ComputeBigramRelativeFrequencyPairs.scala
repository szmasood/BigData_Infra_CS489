package ca.uwaterloo.cs.bigdata2016w.szmasood.assignment2;

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._
import scala.collection.mutable.ListBuffer

class Conf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
}

case class Bigram(left: String, right: String)
object Bigram {
  implicit def orderingByLeft[A <: Bigram] : Ordering[A] = {
    Ordering.by(fk => (fk.left, fk.right))
  }
}

// Reference: http://codingjunkie.net/spark-secondary-sort/
class LeftKeyPartitioner (partitions: Int) extends Partitioner {

  override def numPartitions: Int = partitions
  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Bigram]
    (k.left.hashCode() & Integer.MAX_VALUE) % numPartitions
  }
}


object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
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
        var bigrams = ListBuffer [(Bigram,Int)]()
        if (tokens.length > 1) {
          bigrams = bigrams ++ tokens.sliding(2).map(p => (Bigram(p(0),p(1)),1)).toList
          for (i <- 0 until tokens.length - 1) {
            bigrams += ((Bigram(tokens(i),"*"), 1))
          }
          bigrams.toList
        }
        else {
          List()
        }
      })
      .reduceByKey(_+_)
      .repartitionAndSortWithinPartitions(new LeftKeyPartitioner(args.reducers()*4*4))
      .mapPartitions(bi => {
        var bigram = new Bigram("","")
        var res = ListBuffer [(String)]()
        var curr = (bigram, 1)
        var sum = 0f
        var marginal = 0f
        while (bi.hasNext) {
          curr = bi.next()
          sum = curr._2
          bigram = curr._1
          if (bigram.right == "*") {
            res += (s"(${bigram.left}, ${bigram.right})\t${sum}")
            marginal = sum
          }
          else {
            if (marginal != 0f) {
              res += (s"(${bigram.left}, ${bigram.right})\t${sum/marginal}")
            }
            else {
              res += (s"(${bigram.left}, nullnull)\t0")
            }
          }
        }
        res.iterator
      })

    counts.saveAsTextFile(args.output())
  }
}
