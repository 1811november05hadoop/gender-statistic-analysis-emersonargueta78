package com.revature.reduce;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * ReduceNulls
 */
public class SumReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public static volatile String NON_MALE_CODE = null;
    public static AtomicInteger CURRENT_VALUE = new AtomicInteger(Integer.MIN_VALUE);


    /**
     * This reducer will reduce input from two mappers with different
     * input formats. 
     * The first mapper will have key value pair 
     *  {codeForGenderStats:IntWritable}.
     * The second mapper will have key value pair 
     *  {[Country,codeForGenderStats,[data]],IntWritable}
    */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {
        
        for (IntWritable value : values) {
            String theData = key.toString();
            

            /**
             *  This is true if the key and value are the output of the first mapper
             *  The first mapper extracts the codes from the first input file
             * 
             * */
            if(theData.contains("\",\"")){

            }
            



        }
            
        
    }
    
}