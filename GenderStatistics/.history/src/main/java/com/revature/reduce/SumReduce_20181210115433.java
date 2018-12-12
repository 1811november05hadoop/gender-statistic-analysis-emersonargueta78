package com.revature.reduce;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * ReduceNulls
 */
public class SumReduce extends Reducer<Text, Text, Text, IntWritable> {

    public static volatile String CURRENT_VALUE = null;
    public static AtomicInteger CURRENT_VALUE_INT = new AtomicInteger(Integer.MIN_VALUE);


    /**
     * This reducer will reduce input from two mappers with different
     * input formats. 
     * The first mapper will have key value pair 
     *  {codeForGenderStats:IntWritable}.
     * The second mapper will have key value pair 
     *  {[Country,codeForGenderStats,[data]],IntWritable}
    */
    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            
        }
            
        
    }
    
}