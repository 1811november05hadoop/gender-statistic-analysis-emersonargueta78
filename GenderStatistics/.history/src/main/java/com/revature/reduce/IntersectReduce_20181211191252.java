package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * ReduceNulls
 */
public class IntersectReduce extends Reducer<Text, Text, Text, Text> {
    
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
    public static volatile String indicatorName = "";

    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
        
        for (Text value : values) {
            
            /**
             * The key indicates the Indicator Code from the Gender_StatsData.csv
             * or the Series Code from the Gender_StatsSeries.csv
             * The Series Code and Indicator Code are equivalent.
             * 
             * From the first mapper, the appropriate Series code
             * relating to the bussiness question is extracted 
             * from the Gender_StatsSeries.csv.
             * 
             * Using the appropriate Series Code, the data is extracted
             * from the output of the second mapper.
             *  
             */
            if(value.toString().charAt(0) == ':'){
                indicatorName = value.toString();
                for (Text val : values) {
                    valEmit.set(indicatorName+" "+val.toString());
                    keyEmit.set(key+" " +indicatorNameICode);
                    context.write(keyEmit, valEmit);        
                }
            }
        }
    }
    
}