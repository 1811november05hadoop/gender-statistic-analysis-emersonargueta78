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

    public static volatile String NON_NULL_WORD = null;
    public static AtomicInteger CURRENT_VALUE = new AtomicInteger(Integer.MIN_VALUE);


    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {
        
        for (IntWritable value : values) {
            
            try {
                NON_NULL_WORD = key.toString();    
                //if(!NON_NULL_WORD.matches("") && !NON_NULL_WORD.matches("\",")){
                    CURRENT_VALUE.set(value.get());
                    context.write(new Text(NON_NULL_WORD),new IntWritable(CURRENT_VALUE.get()));   
                //}
            } catch (Exception e) {
                continue;
            }
        }
            
        
    }
    
}