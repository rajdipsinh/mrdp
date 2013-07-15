package org.in.hadoop.ch1;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Date: 7/15/13
 * Time: 12:54 PM
 */
public class WordCountDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        String [] jobArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();

        if (jobArgs.length != 2) {
            System.err.println("Usage: CommentWorCount");
            return -2;
        }

        Job job = Job.getInstance(getConf(), "CommentWordCount");

        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path outputPath = new Path(jobArgs[1]);
        FileSystem fileSystem = FileSystem.get(getConf());
        fileSystem.delete(outputPath, true);

        FileInputFormat.addInputPath(job, new Path(jobArgs[0]));
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new WordCountDriver(), args);
        System.exit(status);
    }
}
