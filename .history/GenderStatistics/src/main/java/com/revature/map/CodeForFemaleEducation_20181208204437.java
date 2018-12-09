package com.revature.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * ExtractGenderStats
 */
public class CodeForFemaleEducation extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        List<String> colValsList = new ArrayList<>();    
        StringBuilder stringRow = new StringBuilder();
        //Retrieving all column values
        for (String phrase : line.split("\",\"")) {
            
            //if(!phrase.matches("") && !phrase.matches("\",")){
            stringRow.append(phrase.replace("\"", "").replace("\",",""))
                     .append(" ");       
            colValsList.add(phrase.replace("\"", "").replace("\",",""));       
            //}        
        }
        //Remove unnecessary data 
        
        
        
    }
    
    public String[] extractCodes() {
        
        return null;
    }
    
}