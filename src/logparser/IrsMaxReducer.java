package logparser;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IrsMaxReducer extends Reducer<Text, AvgMaxTuple, Text, AvgMaxTuple> {
	private AvgMaxTuple result = new AvgMaxTuple();
	@Override
	protected void reduce(Text key, Iterable<AvgMaxTuple> values,Context context)
			throws IOException, InterruptedException {
		
			result.setCpuavg(0);
			result.setCpumax(0);
			result.setMemavg(0);
			result.setMemmax(0);
			result.setCount(0);
			int count = 0;
			float cpusum = 0;
			float memsum = 0;
			float cpumax = 0;
			float memmax = 0;		
		
		for (AvgMaxTuple value : values) {
			if(value.getCpumax() > result.getCpumax()) {
				cpumax=value.getCpumax();
			}
			
			if(value.getMemmax() > result.getMemmax()) {
				memmax=value.getMemmax();
			}
			
			cpusum+=value.getCount() * value.getCpuavg();
			memsum+=value.getCount() * value.getMemavg();
			count+=value.getCount();
			
			result.setEquipKey(value.getEquipKey());
			result.setEVENT_TIME(value.getEVENT_TIME());
			
			result.setCpumax(cpumax);
			result.setMemmax(memmax);
			
			result.setCpuavg(cpusum/count);
			result.setMemavg(memsum/count);
			result.setCount(count);

		}
		context.write(key, result);
	}
}
