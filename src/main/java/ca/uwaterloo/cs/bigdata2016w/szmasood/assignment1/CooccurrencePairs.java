package ca.uwaterloo.cs.bigdata2016w.szmasood.assignment1;

import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import tl.lin.data.pair.PairOfStrings;


public class CooccurrencePairs extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(CooccurrencePairs.class);

    public static enum COUNTER {
        FILE_EXISTS, NUM_LINES
    }

    private static class MyMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
        private static final PairOfStrings PAIR = new PairOfStrings();
        private static final IntWritable ONE = new IntWritable(1);


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
                for (int j = i; j < tokens.size(); j++) {
                    if (i == j) continue;
                    PAIR.set(tokens.get(i), tokens.get(j));
                    context.write(PAIR, ONE);
                    PAIR.set(tokens.get(j), tokens.get(i));
                    context.write(PAIR, ONE);
                }
            }
        }
    }

    private static class MyCombiner extends Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {

        private static final IntWritable SUM = new IntWritable();


        @Override
        public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
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

    private static class MyReducer extends
            Reducer<PairOfStrings, IntWritable, PairOfStrings, DoubleWritable> {
        private static final DoubleWritable SUM = new DoubleWritable();
        private static HashMap<String,Integer> wordCounts = new HashMap<>();


        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            if (context.getCacheFiles() != null
                    && context.getCacheFiles().length > 0) {

                InputStream in = new FileInputStream(new File("./mergedLineCounts"));
                BufferedReader reader = new BufferedReader(new InputStreamReader(in,"UTF-8"));
                String line;
                context.getCounter(COUNTER.FILE_EXISTS).increment(1);
                while ((line = reader.readLine()) != null) {
                    String [] split = line.split("\t");
                    wordCounts.put(split[0],Integer.parseInt(split[1]));
                    context.getCounter(COUNTER.NUM_LINES).increment(1);
                }


            }

            super.setup(context);
        }

        @Override
        public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException, NullPointerException {
            Iterator<IntWritable> iter = values.iterator();
            int sum = 0;
            while (iter.hasNext()) {
                sum += iter.next().get();
            }

            double pmi = 0.0;
            if (sum >= 10) {
                int px = wordCounts.get(key.getLeftElement().toString());
                int py = wordCounts.get(key.getRightElement().toString());
                pmi = Math.log10 (((double)(sum)/(px * py)) * wordCounts.get("numLines*"));
                SUM.set(pmi);
                context.write(key, SUM);
            }
        }
    }

    protected static class MyPartitioner extends Partitioner<PairOfStrings, IntWritable> {
        @Override
        public int getPartition(PairOfStrings key, IntWritable value, int numReduceTasks) {
            return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    /**
     * Creates an instance of this tool.
     */
    public CooccurrencePairs() {}

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

        LOG.info("Tool: " + CooccurrencePairs.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + args.output);
        LOG.info(" - number of reducers: " + args.numReducers);

        Job job = Job.getInstance(getConf());


        job.addCacheFile(new URI("/tmp/mergedLineCounts#mergedLineCounts"));

        job.setJobName(CooccurrencePairs.class.getSimpleName());
        job.setJarByClass(CooccurrencePairs.class);

        // Delete the output directory if it exists already.
        Path outputDir = new Path(args.output);
        FileSystem.get(getConf()).delete(outputDir, true);
        job.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(job, new Path(args.input));
        FileOutputFormat.setOutputPath(job, new Path(args.output));

        job.setMapOutputKeyClass(PairOfStrings.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(PairOfStrings.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyCombiner.class);
        job.setReducerClass(MyReducer.class);
        job.setPartitionerClass(MyPartitioner.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        job.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 64);
        job.getConfiguration().set("mapreduce.map.memory.mb", "3072");
        job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
        job.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
        job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");


        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }

}