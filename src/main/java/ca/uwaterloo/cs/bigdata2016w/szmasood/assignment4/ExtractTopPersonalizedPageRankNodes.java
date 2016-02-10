package ca.uwaterloo.cs.bigdata2016w.szmasood.assignment4;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.log4j.Logger;

import scala.tools.nsc.backend.icode.TypeKinds;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.pair.PairOfIntFloat;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectFloat;
import tl.lin.data.pair.PairOfStringInt;
import tl.lin.data.queue.TopScoredObjects;

public class ExtractTopPersonalizedPageRankNodes extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ca.uwaterloo.cs.bigdata2016w.szmasood.assignment4.ExtractTopPersonalizedPageRankNodes.class);
  private static final String SOURCE_NODES = "node.src";
  private static final ArrayList<String> answers = new ArrayList<>();

  private static class MyMapper extends
      Mapper<IntWritable, PageRankNode, PairOfInts, FloatWritable> {
    private static ArrayList<TopScoredObjects<Integer>> queue;
    private static ArrayList<String> sourceNode = null;


    @Override
    public void setup(Context context) throws IOException {
      int k = context.getConfiguration().getInt("n", 100);
      queue = new ArrayList<>();
      String s = context.getConfiguration().get(SOURCE_NODES,"");
      sourceNode = new ArrayList<>();
      String [] srces = s.split(",");
      for (String src: srces) {
        sourceNode.add(src);
        queue.add(new TopScoredObjects<Integer>(k));
      }
    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context) throws IOException,
        InterruptedException {

      for (int i =0; i < sourceNode.size(); i++) {
        queue.get(i).add(node.getNodeId(), node.getPageRank().get(i));
      }

    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      PairOfInts key = new PairOfInts();
      FloatWritable value = new FloatWritable();
      int i =0;

      for (TopScoredObjects<Integer> qu : queue) {
        for (PairOfObjectFloat<Integer> pair : qu.extractAll()) {
          key.set(i,pair.getLeftElement());
          value.set(pair.getRightElement());
          context.write(key, value);
        }
        i++;
      }
    }
  }

  protected static class MyPartitioner extends Partitioner<PairOfInts, FloatWritable> {
    @Override
    public int getPartition(PairOfInts key, FloatWritable value, int numReduceTasks) {
      return (String.valueOf(key.getLeftElement()).hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  private static class MyReducer extends
      Reducer<PairOfInts, FloatWritable, Text, Text> {
    private static ArrayList<TopScoredObjects<Integer>> queue;
    private static ArrayList<String> sourceNode = null;
    private static final IntWritable prev = new IntWritable(-1);
    private final static ArrayList<PairOfIntFloat> PAIRS = new ArrayList<>();


    @Override
    public void setup(Context context) throws IOException {
      int k = context.getConfiguration().getInt("n", 100);
      queue = new ArrayList<>();
      String s = context.getConfiguration().get(SOURCE_NODES,"");
      sourceNode = new ArrayList<>();
      String [] srces = s.split(",");
      for (String src: srces) {
        sourceNode.add(src);
        queue.add(new TopScoredObjects<Integer>(k));
      }

      prev.set(-1);

    }

    @Override
    public void reduce(PairOfInts nid, Iterable<FloatWritable> iterable, Context context)
        throws IOException {
      Iterator<FloatWritable> iter = iterable.iterator();

      while (iter.hasNext()) {
        float pr = iter.next().get();
        if (nid.getLeftElement() != prev.get() && prev.get() != -1) {
          for (PairOfIntFloat prs : PAIRS) {
            queue.get(prev.get()).add(prs.getLeftElement(),prs.getRightElement());
          }
          PAIRS.clear();
        }
        PAIRS.add(new PairOfIntFloat(nid.getRightElement(), pr));
        prev.set(nid.getLeftElement());
      }

    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      IntWritable key = new IntWritable();
      FloatWritable value = new FloatWritable();
      ArrayList<String> vals = null;

      for (PairOfIntFloat prs : PAIRS) {
        queue.get(prev.get()).add(prs.getLeftElement(), prs.getRightElement());
      }
      prev.set(-1);

      int i =0;
      String val = "";

      for (TopScoredObjects<Integer> qu : queue) {
        vals = new ArrayList<>();
        for (PairOfObjectFloat<Integer> pair : qu.extractAll()) {
          key.set(pair.getLeftElement());
          value.set((float) StrictMath.exp(Double.parseDouble(String.valueOf(pair.getRightElement()))));
          val =  String.format("%.5f %d", value.get(), key.get());
          vals.add(val);
        }
//        System.out.println ("Source: " + sourceNode.get(i));
        context.write(new Text("Source: " + sourceNode.get(i)), new Text(""));
        answers.add("Source: " + sourceNode.get(i));

        for (int m = 0; m < vals.size(); m++) {
//          System.out.println (vals.get(m));
          context.write(new Text(vals.get(m)), new Text(""));
          answers.add(vals.get(m));
        }
//        System.out.println();
        context.write(new Text(""), new Text(""));
        answers.add("");
        i++;
      }

    }
  }

  public ExtractTopPersonalizedPageRankNodes() {
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String TOP = "top";
  private static final String SOURCES = "sources";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("top n").create(TOP));
    options.addOption(OptionBuilder.withArgName("sources").hasArg()
            .withDescription("Sources").create(SOURCES));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(TOP) || !cmdline.hasOption(SOURCES)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int n = Integer.parseInt(cmdline.getOptionValue(TOP));
    String sources = cmdline.getOptionValue(SOURCES);

    LOG.info("Tool name: " + ca.uwaterloo.cs.bigdata2016w.szmasood.assignment4.ExtractTopPersonalizedPageRankNodes.class.getSimpleName());
    LOG.info(" - input: " + inputPath);
    LOG.info(" - output: " + outputPath);
    LOG.info(" - top: " + n);
    LOG.info(" - sources: " + sources);

    Configuration conf = getConf();
    conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
    conf.setInt("n", n);
    conf.set(SOURCE_NODES, sources);

    Job job = Job.getInstance(conf);
    job.setJobName(ca.uwaterloo.cs.bigdata2016w.szmasood.assignment4.ExtractTopPersonalizedPageRankNodes.class.getName() + ":" + inputPath);
    job.setJarByClass(ca.uwaterloo.cs.bigdata2016w.szmasood.assignment4.ExtractTopPersonalizedPageRankNodes.class);

    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(PairOfInts.class);
    job.setMapOutputValueClass(FloatWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    job.setPartitionerClass(MyPartitioner.class);

    // Delete the output directory if it exists already.
    FileSystem.get(conf).delete(new Path(outputPath), true);

    job.waitForCompletion(true);

    Path dstPath = new Path("/tmp/mrg3d12");
    FileSystem.get(getConf()).delete(dstPath, true);

    FileSystem fs = FileSystem.get(getConf());
    Path srcPath = FileOutputFormat.getOutputPath(job);

    FileUtil.copyMerge(fs, srcPath, fs, dstPath, false, getConf(), null);

    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(dstPath)));
    String line;
    line=br.readLine();
    while (line != null){
      System.out.println(line);
      line=br.readLine();
    }

    FileSystem.get(getConf()).delete(dstPath, true);

    return 0;
  }


  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new ca.uwaterloo.cs.bigdata2016w.szmasood.assignment4.ExtractTopPersonalizedPageRankNodes(), args);
    System.exit(res);
  }

}
