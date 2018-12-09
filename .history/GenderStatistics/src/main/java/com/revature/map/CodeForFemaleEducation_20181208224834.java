package com.revature.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import com.revature.Util.Cleaning;
import com.revature.Util.GenderStatsCols;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * ExtractGenderStats
 */
public class CodeForFemaleEducation extends Mapper<LongWritable, Text, Text, IntWritable> {
    //final private static String regex = "(.*)education(.*).*(.*)female(.*)";
    final private static String regex1 = "(.*)education(.*)";
    final private static String regex2 = "";

    @Override
    protected void map(LongWritable key, Text value,Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        
        Entry<StringBuilder,ArrayList<String>> entry = new Cleaning().splitAndClean(line);
        
        StringBuilder stringRow = entry.getKey();
        ArrayList<String> colValsList = entry.getValue();

        //Get series code for rows matching female and education
        if(stringRow.toString().matches(regex1) && !stringRow.toString().matches(regex2)){
            getSeriesCode(colValsList,context);
        }
        
    }
    
    private void getSeriesCode(List<String> colValsList, Context context)
     throws IOException, InterruptedException {
        
        //Getting the series code
        StringBuilder graduateData = new StringBuilder()
                                    .append(colValsList.get(GenderStatsCols.Series.SERIES_CODE));
        System.out.println("the graduate data: "+graduateData.toString());
    
        //Writing the valuable col vals to context
        context.write(new Text(graduateData.toString()), new IntWritable(1));
        
        
    }
    
}