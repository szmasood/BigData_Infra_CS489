package ca.uwaterloo.cs.bigdata2016w.szmasood.assignment3;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.log4j.net.SyslogAppender;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import scala.tools.nsc.typechecker.PatternMatching;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.*;

public class BuildInvertedIndexCompressed extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

  private static class MyMapper extends Mapper<LongWritable, Text, PairOfStringInt, IntWritable> {
    private static final Text WORD = new Text();
    private static final IntWritable TF = new IntWritable();
    private static final Object2IntFrequencyDistribution<String> COUNTS =
        new Object2IntFrequencyDistributionEntry<String>();

    @Override
    public void map(LongWritable docno, Text doc, Context context)
        throws IOException, InterruptedException {
      String text = doc.toString();

      // Tokenize line.
      List<String> tokens = new ArrayList<String>();
      StringTokenizer itr = new StringTokenizer(text);
      while (itr.hasMoreTokens()) {
        String w = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
        if (w.length() == 0) continue;
        tokens.add(w);
      }

      // Build a histogram of the terms.
      COUNTS.clear();
      for (String token : tokens) {
        COUNTS.increment(token);
      }

      // Emit postings.
      for (PairOfObjectInt<String> e : COUNTS) {
        WORD.set(e.getLeftElement());
        TF.set(e.getRightElement());
        context.write(new PairOfStringInt(WORD.toString(), (int)docno.get()), TF);
      }
    }
  }

  protected static class MyPartitioner extends Partitioner<PairOfStringInt, IntWritable> {
    @Override
    public int getPartition(PairOfStringInt key, IntWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  private static class MyReducer extends
      Reducer<PairOfStringInt, IntWritable, Text, BytesWritable> {

    private final static Text TPREV = new Text("!nullnual*");
    private final static IntWritable PREVDOC = new IntWritable(-1);
    private final static BytesWritable P = new BytesWritable();
    private final static ArrayList<PairOfInts> PAIRS = new ArrayList<>();

    @Override
    public void reduce(PairOfStringInt key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iter = values.iterator();

      int docId = key.getRightElement();
      int gap = docId;
      DataOutputBuffer outputBuffer = new DataOutputBuffer();

      while (iter.hasNext()) {
        if (!key.getLeftElement().equals(TPREV.toString()) && !TPREV.toString().equals("!nullnual*")) {
          WritableUtils.writeVInt(outputBuffer, PAIRS.size());
          for (PairOfInts prs : PAIRS) {
            WritableUtils.writeVInt(outputBuffer, prs.getLeftElement());
            WritableUtils.writeVInt(outputBuffer, prs.getRightElement());
          }
          P.setCapacity(outputBuffer.getData().length);
          P.set(outputBuffer.getData(),0, outputBuffer.getLength());
          context.write(TPREV, P);
          P.set(new BytesWritable());
          PAIRS.clear();
          PREVDOC.set(-1);
          outputBuffer.reset();
        }

        if (docId != PREVDOC.get() && PREVDOC.get() != -1) {
          gap -= PREVDOC.get();
        }
        PAIRS.add(new PairOfInts(gap,iter.next().get()));
        TPREV.set(key.getLeftElement());
        PREVDOC.set(docId);
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      DataOutputBuffer outputBuffer = new DataOutputBuffer();
      WritableUtils.writeVInt(outputBuffer, PAIRS.size());
      for (PairOfInts prs : PAIRS) {
        WritableUtils.writeVInt(outputBuffer, prs.getLeftElement());
        WritableUtils.writeVInt(outputBuffer, prs.getRightElement());
      }
      P.setCapacity(outputBuffer.getData().length);
      P.set(outputBuffer.getData(),0, outputBuffer.getLength());
      context.write(TPREV, P);
      P.set(new BytesWritable());
      PAIRS.clear();
      PREVDOC.set(-1);
      TPREV.set("!nullnual*");
      outputBuffer.reset();
    }

  }

  private BuildInvertedIndexCompressed() {}

  public static class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    public String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    public String output;

    @Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
    public int numReducers = 1;
  }

  /**
   * Runs this tool.
   */
  public int run(String[] argv) throws Exception {
    Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + BuildInvertedIndexCompressed.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);

    Job job = Job.getInstance(getConf());
    job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
    job.setJarByClass(BuildInvertedIndexCompressed.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(PairOfStringInt.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);
//    job.setOutputFormatClass(TextOutputFormat.class);


    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    job.setPartitionerClass(MyPartitioner.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildInvertedIndexCompressed(), args);
  }
}
