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
 * ColumnValueMapper
 */
public class ColumnValueMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    //Used to extract rows with words education and female
    // final private static String searchEducationFemaleCodes = "(.*)Educational(.*).*(.*)completed(.*).*(.*)female(.*)";
    // final private static String searchEducationCodes = "(.*)education(.*)";


    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();

        Entry<StringBuilder,ArrayList<String>> entry = new Cleaning().splitAndClean(line);
        
        StringBuilder stringRow = entry.getKey();
        ArrayList<String> colValsList = entry.getValue();

        getCountryAndYearData(colValsList,context,6);
        
        
        
    }

    private void getCountryAndYearData(List<String> colValsList, Context context,int rangeOfYears)
     throws IOException, InterruptedException {
        
        //Getting the country
        StringBuilder graduateData = new StringBuilder();

        String indicatorCode = colValsList.get(GenderStatsCols.Data.INDICATOR_CODE).trim();
        String country = colValsList.get(GenderStatsCols.Data.INDICATOR_CODE).trim();

        //Getting the last 6 years of values 
        for (int i = GenderStatsCols.Data.YEAR.length-1,j=0;j<rangeOfYears; i--,j++) {
            graduateData.append(colValsList.get(GenderStatsCols.Data.YEAR[i]))
                        .append("");
            
        }
    
        //Writing the valuable col vals to context
        context.write(new Text(indicatorCode+":"+
                               country+":"+
                               graduateData.toString()),
                               new IntWritable(1));
        
    }
    
}