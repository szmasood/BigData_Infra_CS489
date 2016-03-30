package ca.uwaterloo.cs.bigdata2016w.szmasood.assignment7;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.*;

public class BuildInvertedIndexHBase extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildInvertedIndexHBase.class);

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
          TableReducer<PairOfStringInt, IntWritable, ImmutableBytesWritable> {

    private final static Text TPREV = new Text("!nullnual*");
    private final static BytesWritable P = new BytesWritable();
    private final static ArrayList<PairOfInts> PAIRS = new ArrayList<>();
    public static final String [] FAMILIES = { "p" };
    public static final byte[] CF = FAMILIES[0].getBytes();
    public static final IntWritable LASTDOCID = new IntWritable(0);



    @Override
    public void reduce(PairOfStringInt key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iter = values.iterator();

      int docId = key.getRightElement();
      LASTDOCID.set(docId);
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

          Put put = new Put(Bytes.toBytes(TPREV.toString()));
          put.add(CF,Bytes.toBytes(docId), P.getBytes());

          context.write(null, put);
          P.set(new BytesWritable());
          PAIRS.clear();
          outputBuffer.reset();
        }

        PAIRS.add(new PairOfInts(docId,iter.next().get()));
        TPREV.set(key.getLeftElement());
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
      Put put = new Put(Bytes.toBytes(TPREV.toString()));
      put.add(CF,Bytes.toBytes(LASTDOCID.get()), P.getBytes());

      context.write(null, put);

      P.set(new BytesWritable());
      PAIRS.clear();
      TPREV.set("!nullnual*");
      outputBuffer.reset();
    }

  }

  private BuildInvertedIndexHBase() {}

  public static class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    public String input;

    @Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
    public int numReducers = 1;

    @Option(name = "-table", metaVar = "[name]", required = true, usage = "HBase table")
    public String table;

    @Option(name = "-config", metaVar = "[path]", required = true, usage = "HBase config")
    public String config;
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

    LOG.info("Tool: " + BuildInvertedIndexHBase.class.getSimpleName());
    LOG.info(" - input path: " + args.input);

    Configuration conf = getConf();
    conf.addResource(new Path(args.config));

    Configuration hbaseConfig = HBaseConfiguration.create(conf);
    HBaseAdmin admin = new HBaseAdmin(hbaseConfig);

    if (admin.tableExists(args.table)) {
      LOG.info(String.format("Table '%s' exists: dropping table and recreating.", args.table));
      LOG.info(String.format("Disabling table '%s'", args.table));
      admin.disableTable(args.table);
      LOG.info(String.format("Droppping table '%s'", args.table));
      admin.deleteTable(args.table);
    }

    HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(args.table));
    for (int i = 0; i < 1; i++) {
      HColumnDescriptor hColumnDesc = new HColumnDescriptor("p");
      tableDesc.addFamily(hColumnDesc);
    }
    admin.createTable(tableDesc);
    LOG.info(String.format("Successfully created table '%s'", args.table));

    admin.close();

    Job job = Job.getInstance(getConf());
    job.setJobName(BuildInvertedIndexHBase.class.getSimpleName());
    job.setJarByClass(BuildInvertedIndexHBase.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));

    job.setMapOutputKeyClass(PairOfStringInt.class);
    job.setMapOutputValueClass(IntWritable.class);


    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    job.setPartitionerClass(MyPartitioner.class);

    TableMapReduceUtil.initTableReducerJob(args.table, MyReducer.class, job);


    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildInvertedIndexHBase(), args);
  }
}
