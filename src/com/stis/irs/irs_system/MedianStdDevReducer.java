package com.stis.irs.irs_system;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MedianStdDevReducer extends Reducer<Text, FloatWritable, Text, MedianStdDevTuple>{
                   
	private MedianStdDevTuple result= new MedianStdDevTuple();
	private ArrayList<Float> valList=new ArrayList<Float>();
	private String bKey="";
    private Text bKeyText=new Text();
    int seq=0;
    float groupcnt=0;
    float groupsum=0;
    float mean=0;
    float sumSqares=0;

    
    
    
	@Override
	protected void reduce(Text key, Iterable<FloatWritable> values, Context context)
			throws IOException, InterruptedException {
          for (FloatWritable val : values) {
        	   if(key.toString().equals(bKey)) {
	        	   valList.add((float)val.get());
	        	   groupsum = groupsum+(float)val.get();
	        	   groupcnt++;
             	  System.out.println("##if groupsum##"+groupsum);
             	  System.out.println("##if groupcnt##"+groupcnt);	        	   
        	   }else {
                      if(seq != 0) {
                     	  System.out.println("##else groupsum##"+groupsum);
                     	  System.out.println("##else groupcnt##"+groupcnt);	 
	        			   Collections.sort(valList);
		        		   if(groupcnt % 2== 0) {
		        			   result.setMedian((valList.get((int) groupcnt / 2 - 1)+
		        					   valList.get((int) groupcnt / 2)) / 2.0f)  ;
		        		   }else {
		        			   //홀수값이면 중앙값을 표준값으로 설정한다.
		        			   result.setMedian(valList.get((int) groupcnt / 2));
		        		   }
		        		   mean=groupsum/groupcnt;
		        		   for (Float f : valList) {
							  sumSqares=sumSqares+((f-mean) * (f-mean));
						   }	
		        		   
		        		   result.setStdDev((float) Math.sqrt(sumSqares/(groupcnt)));
		        		   context.write(bKeyText, result);
                     }
           	       
            	   groupsum=0.0f;
            	   groupcnt=0.0f;
            	   mean=0.0f;
            	   sumSqares=0.0f;
               	   valList.clear();
               	   valList.add((float)val.get());
               	   groupsum = groupsum+(float)val.get();
            	   groupcnt++;
	    	   }
        	   bKey=key.toString();
        	   bKeyText.set(bKey);
        	   seq++;

   		   }
	}
    @Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		if(groupcnt != 0) {
			   Collections.sort(valList);
 		   if(groupcnt % 2== 0) {
 			   result.setMedian((valList.get((int) groupcnt / 2 - 1)+
 					   valList.get((int) groupcnt / 2)) / 2.0f)  ;
 		   }else {
 			   //홀수값이면 중앙값을 표준값으로 설정한다.
 			   result.setMedian(valList.get((int) groupcnt / 2));
 		   }
 		   mean=groupsum/groupcnt;
 		   for (Float f : valList) {
				  sumSqares=sumSqares+((f-mean) * (f-mean));
			   }	
 		   result.setStdDev((float) Math.sqrt(sumSqares/(groupcnt)));
 		   context.write(bKeyText, result);
		}

	}
}
