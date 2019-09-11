package com.stis.irs.irs_system;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IrsMaxMapper extends Mapper<LongWritable, Text, Text, AvgMaxTuple> {

	private Text outKey = new Text();
	private FloatWritable result = new FloatWritable();



	private AvgMaxTuple outTuple = new AvgMaxTuple();
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
	
		String columns[]=value.toString().split(",");
		
		try {
			if(columns.length > 0 && columns!=null) {
				outKey.set(columns[2]+",");
				if(columns[11]!=null && columns[13]!=null && columns[16]!=null && columns[18]!=null) {
					outTuple.setEquipKey(columns[0]);
					outTuple.setEVENT_TIME(columns[3]);
					outTuple.setCpumax(Float.parseFloat(columns[18]));
					outTuple.setCpuavg(Float.parseFloat(columns[16]));					
					outTuple.setMemmax(Float.parseFloat(columns[13]));
					outTuple.setMemavg(Float.parseFloat(columns[11]));
					outTuple.setCount(1);
			    }
			}
			context.write(outKey, outTuple);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}