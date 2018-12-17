package com.revature.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Q1Mapper
 */
public class Q1Mapper extends Mapper<IntWritable,Text,MapWritable,IntWritable>{
    
    @Override
    protected void map(IntWritable key, Text value, Mapper<IntWritable, Text, MapWritable, IntWritable>.Context context)
    throws IOException, InterruptedException {
        String line = value.toString();
        
        Map map = new MapWritable();

        String indicatorCode = "";

        for(String colVal : line.split(",")){
                        
        }    
        context.write(new MapWritable(),new IntWritable(1));
    }
    
}