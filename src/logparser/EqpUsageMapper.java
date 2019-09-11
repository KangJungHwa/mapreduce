package logparser;


import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;



public class EqpUsageMapper extends Mapper<LongWritable, Text, Text, Text>{
	private Text outputKey =new Text();
	private Text newValue= new Text();
	public final static String DATA_TAG="B";
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		InputSplit split = context.getInputSplit();
	    Class<? extends InputSplit> splitClass = split.getClass();

	    FileSplit fileSplit = null;
	    if (splitClass.equals(FileSplit.class)) {
	        fileSplit = (FileSplit) split;
	    } else if (splitClass.getName().equals(
	            "org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {
	         try {
	            Method getInputSplitMethod = splitClass
	                    .getDeclaredMethod("getInputSplit");
	            getInputSplitMethod.setAccessible(true);
	            fileSplit = (FileSplit) getInputSplitMethod.invoke(split);
	        } catch (Exception e) {
	            // wrap and re-throw error
	            throw new IOException(e);
	        }

	    }
	      String filename = fileSplit.getPath().getName();
	    
	      //String addColumns=filename.substring(33,39)+","+filename.substring(15,28)+",";
	      
	      String addColumns=filename.substring(48,49)+","+filename.substring(33,39)+","+filename.substring(15,28)+",";//  LSM_R_ID,SYSNAME,EVENT_TIME
	      
			if(key.get()>0) {
				newValue.set(value.toString().replace("\n", ""));
				
				newValue.set(addColumns.concat(newValue.toString()));				

				newValue.toString().concat("\n");
				
				String columns[]=newValue.toString().split(",");
				try {
					if(columns.length>17 && columns!=null) {
						if((columns[3].equals("1") || columns[3].equals("2") || columns[3].equals("3")
						   || columns[3].equals("4") || columns[3].equals("5") || columns[3].equals("8")
						   || columns[3].equals("9") || columns[3].equals("aa") || columns[3].equals("12")
						   || columns[3].equals("13") || columns[3].equals("14") || columns[3].equals("15")
						   || columns[3].equals("16") || columns[3].equals("17") || columns[3].equals("18")
						   || columns[3].equals("19") || columns[3].equals("20") || columns[3].equals("21")
						   || columns[3].equals("24") || columns[3].equals("25") || columns[3].equals("27")
						   || columns[3].equals("30") || columns[3].equals("32") || columns[3].equals("33")
						   || columns[3].equals("34") || columns[3].equals("53") || columns[3].equals("55")
						   || columns[3].equals("57") || columns[3].equals("60") || columns[3].equals("74")
						   || columns[3].equals("75") || columns[3].equals("77") || columns[3].equals("78")
						   || columns[3].equals("92") || columns[3].equals("93") || columns[3].equals("185"))
						   && columns[0].equals("1"))
						{
							outputKey.set(columns[1]+"_"+DATA_TAG);
							context.write(outputKey, newValue);
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
	}
}