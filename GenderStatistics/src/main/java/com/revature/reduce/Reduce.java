package com.revature.reduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * ReduceNulls
 */
public class Reduce extends Reducer<Text, Text, Text, Text> {
    
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
    
    public static volatile String indicatorStr = "";

    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Context context) throws IOException, InterruptedException {
        ArrayList<Text> valueHolder = new ArrayList<>();
        
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
            if(value.toString().startsWith(":")){
                indicatorStr = value.toString();
            }
            valueHolder.add(value);
        }

        for (Text val : valueHolder) {
            if(val.toString().startsWith(":")){}
            else{
                keyEmit = new Text(indicatorStr);
                valEmit = new Text(val);
                System.out.println("key "+ key+" value "+val+" indicator name: "+ indicatorStr);
                context.write(keyEmit,valEmit);
            }
        }
        
    }
    
}