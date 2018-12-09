package com.revature.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.revature.Util.GenderStatsCols;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * ExtractGenderStats
 */
public class CodeForFemaleEducation extends Mapper<LongWritable, Text, Text, IntWritable> {
    final private static String regex = "(.*)education(.*).*(.*)female(.*)";

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        List<String> colValsList = new ArrayList<>();    
        StringBuilder stringRow = new StringBuilder();
        //Retrieving all column values
        for (String phrase : line.split("\",\"")) {
            
            stringRow.append(phrase.replace("\"", "").replace("\",",""))
                     .append(" ");       
            colValsList.add(phrase.replace("\"", "").replace("\",",""));       
        }

        //Get series code for rows matching female and education
        if(stringRow.toString().matches(regex)){
            getSeriesCode(colValsList,context);
        }
        
    }
    
    private void getSeriesCode(List<String> colValsList, Context context)
     throws IOException, InterruptedException {
        
        //Getting the series code
        StringBuilder graduateData = new StringBuilder()
                                    .append(colValsList.get(GenderStatsCols.Series.SERIES_CODE))
                                    .append(" ");
        
    
        //Writing the valuable col vals to context
        context.write(new Text(graduateData.toString()), new IntWritable(1));
        
        
    }
    
}