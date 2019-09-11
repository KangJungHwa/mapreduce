package com.stis.irs.irs_system;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class EqpUsageDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
		
		if (otherArgs.length != 3) {
			System.err.println("Usage: EqpUsageDriver <metadata> <in> <out>");
			System.exit(2);
		}

		Job job = new Job(getConf(), "EqpUsageDriver");

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		job.setJarByClass(EqpUsageDriver.class);
		job.setReducerClass(EqpUsageReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]),
		TextInputFormat.class, EqpCodeMapper.class);
		
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
				TextInputFormat.class, EqpUsageMapper.class);

		job.waitForCompletion(true);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		
		System.out.println("## RESULT3------:" );
		int res = ToolRunner.run(new Configuration(), new EqpUsageDriver(),
				args);
		System.out.println("## RESULT4------:" + res);

	}
	
	
}
