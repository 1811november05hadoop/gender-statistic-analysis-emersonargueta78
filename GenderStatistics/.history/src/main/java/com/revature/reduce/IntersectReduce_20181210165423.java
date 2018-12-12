package com.revature.reduce;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * ReduceNulls
 */
public class IntersectReduce extends Reducer<Text, Text, Text, Text> {

    Text valEmit = new Text();
    String merge = "";

    /**
     * This reducer will reduce input from two mappers with different
     * input formats. 
     * The first mapper will have key value pair 
     *  {codeForGenderStats:IntWritable}.
     * The second mapper will have key value pair 
     *  {[Country,codeForGenderStats,[data]],IntWritable}
    */
    public static volatile String indicatorNameICode = "";
    public static volatile String dataValues = "";
    public static volatile String indicatorName = "";
    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
        
        for (Text value : values) {
            System.out.println("the value: "+ value);
        }  
        
        merge = indicatorName+" "+dataValues; 
    
        valEmit.set(merge);
        context.write(new Text(indicatorNameICode), valEmit);
        
    }
    
}