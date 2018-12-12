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
        int indicatorCodeCount = 0;
        String theData = "";
        String theIndicatorCode = "";
        Text emitKey = new Text();
        for (Text value : values) {
            theData = key.toString();
            //System.out.println("val: " + value.toString() +" key: " + key.toString());
            
            // if(theData.contains(":")){
                //String [] colVals = theData.split(":");
                emitKey.set(theData);       
            // }
            // else{

            // }
            indicatorCodeCount++;
        }
        context.write(emitKey, new IntWritable(indicatorCodeCount));
        indicatorCodeCount = 0;
        //context.write(new Text(CURRENT_VALUE),new IntWritable(CURRENT_VALUE_INT.get()));;
        
        // for (IntWritable value : values) {
        //     String theData = key.toString();
            
            

        //     /**
        //      *  This is true if the key and value are the output of the first mapper
        //      *  The first mapper extracts the codes from the first input file
        //      * 
        //      * */
        //     if(theData.contains("\",\"")){
        //         String [] colVals = theData.split("\",\"");
                
        //     }
        //     else{
        //         String [] colVals = theData.split(":");

        //     }
            



        // }
            
        
    }
    
}