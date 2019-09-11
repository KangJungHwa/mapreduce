package logparser;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class JoinPartitioner extends Partitioner<SysNameKey,Text> { 
    @Override 
    public int getPartition(SysNameKey sysnamekey, Text text, int numPartitions) { 
        return sysnamekey.getSysanme().hashCode() % numPartitions; 
    } 
} 
