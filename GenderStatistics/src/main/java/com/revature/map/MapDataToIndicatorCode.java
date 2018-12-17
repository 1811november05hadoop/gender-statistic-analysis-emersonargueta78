package com.revature.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import com.revature.Drivers.FemaleEducation;
import com.revature.Util.Cleaning;
import com.revature.Util.GenderStatsCols;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * ColumnValueMapper
 */
public class MapDataToIndicatorCode extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        
        System.out.println("key: "+key+" value: "+ value);
        
    }
}