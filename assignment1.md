Assignment 1 - Shayan Masood
============

1
--
1. Pairs
For the pairs implementation, I have 2 MapReduce jobs found in PairsPMI.java and CooccurrencePairs.java. The purpose of PairsPMI is to find number of lines every word occurs on and the total number of lines in the corpus. To do this, the mapper input takes in Shakespeare data/enwiki-20151201-pages-articles-0.1sample.txt data for linux environment/altiscale and outputs a (word,1) for every unique word per line as well as a ("numLines*",1) pair per line. The number of map output records is essentially the summation of number of unique words per line + total number of lines ("numLines*",1) pairs. The map side combiner then aggregates any spilled to disk intermediate key value pairs (map output pairs) by grouping by key and summing the values if invoked. The partially aggregated pairs then make their way to the reducer where further summation for all reduce input groups for the particular reducer are done. The number of reducer output records is the number of unique words in the corpus + a numLines pair that indicates the total number of lines in the corpus. The output of this job goes to a tmp file "/tmp/lineCounts" onto hdfs and is loaded through a DistributedCache for the next MapReduce job: CooccurencePairs. CooccurrencePairs takes in Shakespeare data/enwiki-20151201-pages-articles-0.1sample.txt data for linux env/altiscale as mapper input and outputs co-occurring pairs ((w, w*),1) for unique words on every line. The number of map output records is permutations(n,2) * numLines where n is number of unique words on a line. The combiner takes intermediate results from mapper and sums values to find out number of lines the couccuring pair is seen on. These partially summed key value pairs then get to the reducer where a HashMap<String,Integer> is loaded from /tmp/lineCounts in the distributed cache in the setup method defining a map from <word, numOfLinesSeenOn>. All reduce input groups have values summed in the reducer and pmi is calculated per cooccurring pair using pmi = Math.log10 (((float)(sum)/(px * py)) * wordCounts.get("numLines*")) where px and py are number of lines words in the cooccurring pair are seen on. The final output is ((cooccurring pair), PMI) where there exists a record for each cooccurring pair.

Similarly, for the stripes implementation, there are 2 MapReduce jobs: StripesPMI.java and CooccurrenceStripes.java. StripesPMI has the same final output as the pairs implementation, except for the fact that it actually aggregates nu 

while mapper output is a (word, stripe) pair for all unique words. Total number of mapper output records is the total number of unique words in the corpus. These intermediate key value pairs are aggregated in the combiner (if invoked) by doing an element wise sum on the stripes. The reducer takes in stripes belonging to the same key (word) and does an element wise sum to count number of cooccurrences. In the setup method of the reducer, a hashmap containing line counts for words and total number of lines like explained above

2
--
Running time of complete pairs implementation: (8.825 + 48.726) = 57.551 seconds (Ran on linux environment)
Running time of complete stripes implementation: (6.836 + 18.451) = 25.287 seconds (Ran on linux environment)

3
--
Running time of complete pairs without combiners: (11.897 + 50.528) = 62.425 seconds (Ran on linux environment)
Running time of complete stripes without combiners: (6.79 + 20.496) = 27.286 seconds (Ran on linux environment)

4
--
77198 pairs found using hadoop fs -cat output/* | wc -l

5
--
hadoop fs -cat output/* | awk -F '\t' '{print $2,$1}' | sort -g
3.5971175897745367 (anjou, maine)
3.5971175897745367 (maine, anjou)

Both pairs have the same PMI because PMI is symmetrical. Since the log base 10 function is increasing, PMI is larger as P(x,y) increases and P(x)P(y) decreases. Maine of anjou is a name, that is always cooccurring on the same line. This is why it has the highest PMI.

6
--
Three words that have highest PMI with "tears"

hadoop fs -cat output/* | awk -F '\t' '{print $2,$1}' | sort -n | grep tears

2.075765364455672 (tears, shed)
2.016787504496334 (tears, salt)
1.1291422518865388 (tears, eyes)

Three words that have highest PMI with "death"

hadoop fs -cat output/* | awk -F '\t' '{print $2,$1}' | sort -n | grep death

1.0842273179991668 (death, father's)
0.718134676579124 (death, die)
0.7021098794516143 (death, life)

7
--
Three words that have the highest PMI with "waterloo"

hadoop fs -cat cs489-2016w-lintool-a1-wiki-pairs/* | awk -F '\t' '{print $2,$1}' | sort -g  | grep waterloo

1.133321185027168 (waterloo, iowa)
0.9984806709114652 (waterloo, ontario)
0.8788823315716799 (waterloo, battle)

Three words that have the highest PMI with "toronto"

hadoop fs -cat cs489-2016w-lintool-a1-wiki-pairs/* | awk -F '\t' '{print $2,$1}' | sort -g  | grep toronto

5.912857621309739E-4 (toronto, lives)
2.2523537759465344 (toronto, leafs)
2.2436501884246556 (toronto, jays)
