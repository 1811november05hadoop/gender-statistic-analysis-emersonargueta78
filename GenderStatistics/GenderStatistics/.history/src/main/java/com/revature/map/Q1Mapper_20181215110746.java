package com.revature.map;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
public class Q1Mapper extends Mapper<LongWritable,Text,MapWritable,MapWritable> {
    final private static Globals globals = new Globals();
    static{
        globals.setYearlyRanges("1960","2016");
    }
    final private static String[] ranges = globals.getYearlyRanges();
    final private static String yearLowerRange = ranges[0];
    final private static String yearUpperRange = ranges[1];

    public static void main(String[] args) {
        
        System.out.println(yearLowerRange + " "+ yearUpperRange);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
        Cleaning cleaning = new Cleaning();
        String line = value.toString();
        
        Entry<StringBuilder,ArrayList<String>> entry = cleaning.splitAndClean(line);
        StringBuilder stringRow = entry.getKey();
        List<String> colVals = entry.getValue();

        
        StringBuilder bq1DataVals = cleaning
        //.getYearlyColumnVals(colVals, new AbstractMap.SimpleImmutableEntry<>(ranges[0], ranges[1]));
        .getYearlyColumnVals(colVals, new AbstractMap.SimpleImmutableEntry<>("1960", "2016"));
        String indicatorCode = colVals.get(GenderStatsCols.Data.INDICATOR_CODE);
        String country = colVals.get(GenderStatsCols.Data.COUNTRY);

        MapWritable mapKey = new MapWritable();
        MapWritable mapVal = new MapWritable();

        mapKey.put(new Text(indicatorCode), new Text(country));
        mapVal.put(new Text(bq1DataVals.toString()),new IntWritable(1));

        context.write(mapKey,mapVal);
    }
    
}