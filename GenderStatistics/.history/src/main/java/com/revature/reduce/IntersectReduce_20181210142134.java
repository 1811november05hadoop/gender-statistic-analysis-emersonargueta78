package com.revature.reduce;

import java.io.IOException;

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
    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
        String dataValues = "";
        String indicatorName = "";
        
        for (Text value : values) {
            String theCol = value.toString();
            
            if(theCol.contains("|")){
                dataValues = value.toString();
            }
            else{
                indicatorName = value.toString();
            }
        }

        if(!indicatorName.isEmpty()){
            merge = indicatorName+ " " + dataValues;
            System.out.println("the merge: "+merge);
            valEmit.set(merge);
            context.write(key, valEmit);
        }   
    }
    
}