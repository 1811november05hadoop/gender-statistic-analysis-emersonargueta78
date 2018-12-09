package com.revature.reduce;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * ReduceNulls
 */
public class ReduceClean extends Reducer<Text, IntWritable, Text, IntWritable> {

    public static volatile String NON_MALE_CODE = null;
    public static AtomicInteger CURRENT_VALUE = new AtomicInteger(Integer.MIN_VALUE);


    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {
        
        for (IntWritable value : values) {
            
            try {
                NON_MALE_CODE = key.toString();    
                if(!NON_MALE_CODE.matches("(MA)")){
                    CURRENT_VALUE.set(value.get());
                    context.write(new Text(NON_MALE_CODE),new IntWritable(CURRENT_VALUE.get()));   
                }
            } catch (Exception e) {
                continue;
            }
        }
            
        
    }
    
}