package com.stis.irs.irs_system;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class LagMapper extends	Mapper<LongWritable, Text, Text, IntWritable> {


	private IntWritable outputValue = new IntWritable();

	private Text outputKey = new Text();

	public void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {

        if(key.get()>0) {
			String[] colums = value.toString().split(",");
			if (colums != null && colums.length > 13) {
				try {
					outputKey.set(colums[13]);
					if (!colums[13].equals("")) {
						int cpuMax = Integer.parseInt(colums[13]);
						if (cpuMax > 0) {
							outputValue.set(cpuMax);
							context.write(new Text(""), outputValue);
							//context.write(outputKey, outputValue);
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
        }
		}

}
