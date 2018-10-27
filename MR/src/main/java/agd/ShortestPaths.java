package agd;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.mapreduce.Counters;

import org.apache.hadoop.fs.FileSystem;

import org.apache.log4j.Logger;


public class ShortestPaths extends Configured implements Tool {

    private Logger logger = Logger.getLogger(this.getClass());
    
    static enum MoreIterations {
        numUpdated
    }
    
    public static class LSPMapper extends Mapper<Object, Text, Text, IntWritable> {
    	private final static IntWritable minDistance = new IntWritable();
		private final Text source = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String line = value.toString();
            String[] nodes = line.split("\\s+");
            Integer dist = Integer.parseInt(nodes[2]);
            if (dist != Integer.MAX_VALUE) {
            	source.set(conf.get("source"));
            	minDistance.set(dist);
			    context.write(source, minDistance);
            }
		}
	}
	
	public static class LSPReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private final IntWritable result = new IntWritable();

		@Override
		public void reduce(final Text key, final Iterable<IntWritable> values, final Context context) throws IOException, InterruptedException {
			int lsp = 0;
			for (final IntWritable val : values) {
				if (val.get() > lsp) lsp = val.get();
			}
			result.set(lsp);
			context.write(key, result);
		}
	}

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        conf.set("source", args[3]);
        
        // Delete output directory, only to ease local development; will not work on AWS. ===========
		final FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(new Path(args[1]))) {
			fileSystem.delete(new Path(args[1]), true);
		}
		if (fileSystem.exists(new Path(args[2]))) {
			fileSystem.delete(new Path(args[2]), true);
		}
		// ================
        
        long numUpdated = 1;
        int numIterations = 1;
        FileSystem hdfs = FileSystem.get(conf);

        while (numUpdated > 0) {
            logger.info("Iteration: " + numIterations);
            String input, output;
            Job job = Job.getInstance(conf, "shortest path");
            if (numIterations == 1) {
                input = args[0];
            } else {
                input = args[1] + "-" + (numIterations - 1);
            }
            output = args[1] + "-" + numIterations;

            job.setJarByClass(ShortestPaths.class);
            job.setMapperClass(ShortestPathsMapper.class);
            job.setReducerClass(ShortestPathsReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            
            job.waitForCompletion(true);

            Counters jobCounters = job.getCounters();
            numUpdated = jobCounters.
                findCounter(MoreIterations.numUpdated).getValue();
            if (numIterations > 1) {
                hdfs.delete(new Path(input), true);
                logger.info("Updated: " + numUpdated);
            }
            numIterations += 1;
        }
        
        String input = args[1] + "-" + (numIterations - 1);
        Job job2 = Job.getInstance(conf, "shortest path");
        job2.setJarByClass(ShortestPaths.class);
        job2.setMapperClass(LSPMapper.class);
		job2.setReducerClass(LSPReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job2, new Path(input));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        
        return job2.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ShortestPaths(), args);
        System.exit(exitCode);
    }
}
