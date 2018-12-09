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
 * ColumnValueMapper
 */
public class ColumnValueMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    //Used to extract rows with words education and female
    static{
        final private static String [] regex = new ExtractGenderStats().extractCodes();

    }
    
    


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
        //Remove unnecessary data 
        if(stringRow.toString().matches(regex1)){
            //Further remove unnececcessary data
            getCountryAndYearData(colValsList,context,6);
        }
        
        
    }

    private void getCountryAndYearData(List<String> colValsList, Context context,int rangeOfYears)
     throws IOException, InterruptedException {
        
        //Getting the country
        StringBuilder graduateData = new StringBuilder()
                                    .append(colValsList.get(GenderStatsCols.COUNTRY))
                                    .append(" ");
        
        //Getting the last 6 years of values 
        for (int i = GenderStatsCols.YEAR.length-1,j=0;j<rangeOfYears; i--,j++) {
            graduateData.append(colValsList.get(GenderStatsCols.YEAR[i]))
                        .append(" ");
            
        }
    
        //Writing the valuable col vals to context
        context.write(new Text(graduateData.toString()), new IntWritable(1));
        
        
    }
    
}