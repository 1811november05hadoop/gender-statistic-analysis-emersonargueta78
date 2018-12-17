package com.revature.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.revature.util.Cleaning;
import com.revature.util.Globals;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Q1Mapper
 */
public class Q1Mapper extends Mapper<LongWritable,Text,MapWritable,IntWritable> {
    final private static String[] ranges = new Globals().getYearlyRanges();
    final private static String yearLowerRange = ranges[0];
    final private static String yearUpperRange = ranges[1];
    
    public static void main(String[] args) {
        String testRow = "\"Arab World\",\"ARB\",\"Access to anti-retroviral drugs, female (%)\",\"SH.HIV.ARTC.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.0154427732646988\",\"0.0687530803351405\",\"0.103438795848702\",\"0.704228689601689\",\"1.01911528204711\",\"1.6988591086665\",\"2.72210741878432\",\"3.58585441218741\",\"5.61150904455506\",\"7.3503927118048\",\"8.64536596419669\",\"10.0595271778428\",\"12.3582578856259\",\"15.2250279997013\",\"17.7513862812732\",\"21.102335739509\",\"\",";
        Entry<StringBuilder, ArrayList<String>> entry = new Cleaning().splitAndClean(testRow);
        StringBuilder rowStr = entry.getKey();
        List<String> colVals = entry.getValue();

        System.out.println("the rowStr: " + rowStr);
        
        int i = 0;
        for (String colVal : colVals) {
            System.out.println("colVal "+(i++)+" : " + colVal);    
        }
        
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
        String line = value.toString();
        Entry<StringBuilder,ArrayList<String>> entry = new Cleaning().splitAndClean(line);
        
        StringBuilder stringRow = entry.getKey();
        List<String> colvals = entry.getValue();

        Map map = new MapWritable();

        String indicatorCode = "";

        for(String colVal : ){

        }    
        context.write(new MapWritable(),new IntWritable(1));
    }
    
}