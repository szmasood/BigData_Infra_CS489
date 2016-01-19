package ca.uwaterloo.cs.bigdata2016w.szmasood.assignment1;

import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.map.HMapKI;
import tl.lin.data.map.HMapStFW;
import tl.lin.data.map.HMapStIW;
import tl.lin.data.map.MapKI;


public class CooccurrenceStripes extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(CooccurrenceStripes.class);

    private static class MyMapper extends Mapper<LongWritable, Text, Text, HMapStIW> {
        private static final HMapStIW MAP = new HMapStIW();
        private static final Text KEY = new Text();

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
                numWords ++;
            }


            for (int i = 0; i < tokens.size(); i++) {
                MAP.clear();
                for (int j = 0; j < tokens.size(); j++) {
                    if (i == j) continue;

                    MAP.increment(tokens.get(j));
                }
                KEY.set(tokens.get(i));
                context.write(KEY,MAP);
            }
        }

    }

    private static class MyCombiner extends Reducer<Text, HMapStIW, Text, HMapStIW> {

        private static final IntWritable SUM = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<HMapStIW> values, Context context)
                throws IOException, InterruptedException {
            Iterator<HMapStIW> iter = values.iterator();
            HMapStIW map = new HMapStIW();

            while (iter.hasNext()) {
                map.plus(iter.next());
            }

            context.write(key, map);
        }
    }



    private static class MyReducer extends Reducer<Text, HMapStIW, Text, HMapStFW> {

        private static HashMap<String,Integer> wordCounts = new HashMap<>();

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            if (context.getCacheFiles() != null
                    && context.getCacheFiles().length > 0) {

                InputStream in = new FileInputStream(new File("./mergedLineCounts2"));
                BufferedReader reader = new BufferedReader(new InputStreamReader(in,"UTF-8"));
                String line;

                while ((line = reader.readLine()) != null) {
                    String [] split = line.split("\t");
                    wordCounts.put(split[0],Integer.parseInt(split[1]));
                }


            }

            super.setup(context);
        }

        @Override
        public void reduce(Text key, Iterable<HMapStIW> values, Context context)
                throws IOException, InterruptedException {
            Iterator<HMapStIW> iter = values.iterator();
            HMapStIW map = new HMapStIW();

            while (iter.hasNext()) {
                map.plus(iter.next());
            }

            HMapStFW writeMap = new HMapStFW();

            double pmi = 0.0;
            for (MapKI.Entry<String> entry : map.entrySet()) {
                String k = entry.getKey();

                if (map.get(k) >= 10) {
                    if (wordCounts.containsKey(key.toString()) && wordCounts.containsKey(k)) {
                        int px = wordCounts.get(key.toString());
                        int py = wordCounts.get(k);
                        pmi = Math.log10 (((double)(map.get(k))/(px * py)) * wordCounts.get("numLines*"));
                        writeMap.put(k, (float) pmi);
                    }
                }
            }
            if (writeMap.size() > 0) {
                context.write(key, writeMap);
            }
        }
    }

    /**
     * Creates an instance of this tool.
     */
    public CooccurrenceStripes() {}

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


        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + args.output);
        LOG.info(" - number of reducers: " + args.numReducers);

        Job job = Job.getInstance(getConf());
        job.setJobName(CooccurrenceStripes.class.getSimpleName());
        job.setJarByClass(CooccurrenceStripes.class);

        job.addCacheFile(new URI("/tmp/mergedLineCounts2#mergedLineCounts2"));

        // Delete the output directory if it exists already.
        Path outputDir = new Path(args.output);
        FileSystem.get(getConf()).delete(outputDir, true);

        job.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(job, new Path(args.input));
        FileOutputFormat.setOutputPath(job, new Path(args.output));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(HMapStIW.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(HMapStFW.class);

        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyCombiner.class);
        job.setReducerClass(MyReducer.class);

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