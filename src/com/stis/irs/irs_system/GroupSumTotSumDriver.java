package com.stis.irs.irs_system;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GroupSumTotSumDriver extends Configured implements Tool{
	@Override
	public int run(String[] args) throws Exception {
		
		Job job=new Job(getConf());
		job.setJobName("ListAggDriver");
		
		job.setJarByClass(GroupSumTotSumDriver.class);
		job.setMapperClass(GroupSumTotSumMapper.class);
		//job.setNumReduceTasks(0);
		job.setReducerClass(GroupSumTotSumReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		return 0;
	}
	public static void main(String[] args) throws Exception{
		int res= ToolRunner.run(new Configuration(), new GroupSumTotSumDriver(), args);
		System.out.println("## RESULT : "+res);
	}
}