package com.revature.map;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import com.revature.util.Cleaning;
import com.revature.util.GenderStatsCols;
import com.revature.util.Globals;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Q1Mapper
 */
public class Q1Mapper extends Mapper<LongWritable,Text,MapWritable,MapWritable> implements Globals{
    private static String[] yearlyRanges;
    private static String[] regexes;
    

    @Override
    protected void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
        Cleaning cleaning = new Cleaning();
        String line = value.toString();
        
        Entry<StringBuilder,ArrayList<String>> entry = cleaning.splitAndClean(line);
        StringBuilder stringRow = entry.getKey();
        List<String> colVals = entry.getValue();

        
        StringBuilder bq1DataVals = cleaning
        .getYearlyColumnVals(colVals, new AbstractMap.SimpleImmutableEntry<>(yearlyRanges[0], yearlyRanges[1]));
        String indicatorName = colVals.get(GenderStatsCols.Data.INDICATOR_NAME);
        String country = colVals.get(GenderStatsCols.Data.COUNTRY);

        
        
        if(
            indicatorName.matches(regexes[0])
            && indicatorName.matches(regexes[1])
        ){
            MapWritable mapKey = new MapWritable();
            
            //Entry<Boolean,Entry<>>
            if(true){
                MapWritable mapVal = new MapWritable();
            
                mapKey.put(new Text(indicatorName), new Text(country));
                mapVal.put(new Text(bq1DataVals.toString()),new IntWritable(1));
                context.write(mapKey,mapVal);
            }
            
        }
        
    }
   

    private boolean dataValsLess30(String dataVals) {
        double [] dataValsIntArr = Stream.of(dataVals.split(","))
                  .mapToDouble(Double::parseDouble)
                  .toArray();
        for (int i = 0; i < dataValsIntArr.length; i++) {
            if(dataValsIntArr[i] > 30.0){
                return false;
            }
        }
        return true;
    }

    @Override
    public void setRegexes(String... regexes) {
        this.regexes = regexes;
    }


    @Override
    public String[] getRegexes() {
        return this.regexes;
    }

    @Override
    public void setYearlyRanges(String... ranges) {
        this.yearlyRanges = ranges;
    }

    @Override
    public String[] getYearlyRanges() {
        return this.yearlyRanges;
    }
    
}