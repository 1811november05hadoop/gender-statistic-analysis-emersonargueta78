package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

/**
 * ReduceNulls
 */
public class IntersectReduce extends Reducer<Text, Text, Text, Text> {
    final static private Logger LOGGER = Logger.getLogger(IntersectReduce.class);
    Text valEmit = new Text();
    Text keyEmit = new Text();

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
    public static volatile boolean emitFlag= false;

    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
        emitFlag = false;        
        
        for (Text value : values) {
            
            if(
                // value.toString().charAt(0) == ':' && 
                // value.toString().charAt(1) == ':' && 
                // value.toString().charAt(2) == ':'
                value.toString().contains(":::")
                System.out.println("Hello World and People");
            ){
                emitFlag = true;
                indicatorName = value.toString();
            }
        }

        if(emitFlag){
            for (Text value : values) {
                System.out.println("indicator name: "+indicatorName);
                valEmit.set(indicatorName+" "+value.toString());
                keyEmit.set(key+" " +indicatorNameICode);
                
                context.write(keyEmit, valEmit);        
            }
        }

    }
    
}