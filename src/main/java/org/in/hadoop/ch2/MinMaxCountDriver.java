package org.in.hadoop.ch2;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MinMaxCountDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		String [] jobArgs = new GenericOptionsParser(args).getRemainingArgs();
		
		if (jobArgs.length != 2) {
			System.err.println("Usage: MinMaxCounterDriver <inPath> <outPath>");
			return -2;
		}     
		
		Job job = Job.getInstance(getConf(), "StackOverflow User Min, Max and Count job");
        job.setJarByClass(MinMaxCountDriver.class);
        
        job.setMapperClass(MinMaxCountMapper.class);
        job.setCombinerClass(MinMaxCountReducer.class);
        job.setReducerClass(MinMaxCountReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MinMaxCountTuple.class);
        
        Path inPath = new Path(jobArgs[0]);
        Path outPath = new Path(jobArgs[1]);
        FileSystem fileSystem = FileSystem.get(getConf());
        fileSystem.delete(outPath, true);
        
        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
        
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitStatus = ToolRunner.run(new MinMaxCountDriver(), args);
		System.exit(exitStatus);
	}
}
