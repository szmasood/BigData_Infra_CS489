package ca.uwaterloo.cs.bigdata2016w.szmasood.assignment1;

/**
 * Created with IntelliJ IDEA.
 * User: shayanmasood
 * Date: 16-01-16
 * Time: 2:22 PM
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;


/**
 * Counts bigrams.
 */
public class PairsPMI extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(PairsPMI.class);

    protected static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private static final Text WORD = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = ((Text) value).toString();

            List<String> tokens = new ArrayList<String>();
            StringTokenizer itr = new StringTokenizer(line);
            int numWords = 0;
            while (itr.hasMoreTokens() && numWords < 100) {
                String w = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
                if (w.length() == 0) continue;
                if (!tokens.contains(w)) {
                    tokens.add(w);
                }
                numWords++;
            }
            for (int i = 0; i < tokens.size(); i++) {
                WORD.set(tokens.get(i));
                context.write(WORD, ONE);
            }

            if (tokens.size() != 0) {
                WORD.set ("numLines*");
                context.write(WORD, ONE);
            }

        }
    }

    private static class MyCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

        private static final IntWritable SUM = new IntWritable();


        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> iter = values.iterator();
            while (iter.hasNext()) {
                sum += iter.next().get();
            }

            SUM.set(sum);
            context.write(key, SUM);
        }
    }

    protected static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final static IntWritable SUM = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {
            int sum = 0;
            Iterator<IntWritable> iter = values.iterator();
            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            if (sum >= 10) {
                SUM.set(sum);
                context.write(key, SUM);
            }
        }
    }

    /**
     * Creates an instance of this tool.
     */
    public PairsPMI() {}

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

        LOG.info("Tool name: " + PairsPMI.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + args.output);
        LOG.info(" - num reducers: " + args.numReducers);

        Job job = Job.getInstance(getConf());
        job.setJobName(PairsPMI.class.getSimpleName());
        job.setJarByClass(PairsPMI.class);

        job.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(job, new Path(args.input));
        FileOutputFormat.setOutputPath(job, new Path("/tmp/lineCounts"));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyCombiner.class);
        job.setReducerClass(MyReducer.class);

        // Delete the output directory if it exists already.
        Path outputDir = new Path("/tmp/lineCounts");
        FileSystem.get(getConf()).delete(outputDir, true);

        Path mergedOutputDir = new Path("/tmp/mergedLineCounts");
        FileSystem.get(getConf()).delete(mergedOutputDir, true);


        job.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 64);
        job.getConfiguration().set("mapreduce.map.memory.mb", "3072");
        job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
        job.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
        job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");


        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);

        FileSystem fs = FileSystem.get(getConf());
        Path srcPath = new Path("/tmp/lineCounts");
        Path dstPath = new Path("/tmp/mergedLineCounts");

        FileUtil.copyMerge(fs, srcPath, fs, dstPath, false, getConf(), null);

        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new PairsPMI(), args);
        ToolRunner.run(new CooccurrencePairs(), args);
    }
}

