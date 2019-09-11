
package com.stis.irs.irs_system;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SysNameWithNumericKeyMapper extends Mapper<LongWritable, Text, SysNameWithNumericKey, Text> {

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String columns[]=value.toString().split(",");
		if(columns!=null && columns.length>0) {
			SysNameWithNumericKey sysKey=new SysNameWithNumericKey();
			sysKey.setSysName(Integer.parseInt(columns[2]));
			sysKey.setCpuAvg(Float.parseFloat(columns[13]));
			context.write(sysKey, value);
		}
		
	}
	
	

}
