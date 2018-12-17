package com.revature.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import com.revature.Util.Cleaning;
import com.revature.Util.GenderStatsCols;
import com.revature.Util.Regex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**
 * Classname: ExtractGenderStats
 * 
 * Description: Extracting the Series Code and Indicator Name
 *              column values that are related to graduation rates
 *              for females from the Gender_StatsSeries.csv
 * 
 */
public class CodeForBussinessQuestion extends Mapper<LongWritable, Text, Text, Text> {
    final private static Logger LOGGER = Logger.getLogger(CodeForBussinessQuestion.class);
    /**
     * These patterns are set in the Drivers
     */
    final private static String firstLevelPattern = new Regex().getRegexes()[0];
    final private static String secondLevelPattern = new Regex().getRegexes()[1];
    // final private static String secondLevelPattern ="(.*)Educational(.*).*(.*)completed(.*).*(.*)female(.*)";
    // final private static String firstLevelPattern = "(.*)education(.*)";
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        
        Entry<StringBuilder,ArrayList<String>> entry = new Cleaning().splitAndClean(line);
        
        StringBuilder stringRow = entry.getKey();
        ArrayList<String> colValsList = entry.getValue();

        //Get series code for rows matching education
        if(stringRow.toString().matches(firstLevelPattern)){
            getSeriesCode(colValsList,context);
        }
    }
    
    /**
     * Description: This helper method extracts the valuable series code
     *              relating to education and females.
     */
    private void getSeriesCode(List<String> colValsList, Context context)
     throws IOException, InterruptedException {
        
        String seriesCode = colValsList.get(
                                            GenderStatsCols.Series.SERIES_CODE
                                           )
                                        .trim();
        
        if(!seriesCode.contains(" ")){
            String indicatorName = colValsList.get(
                                                   GenderStatsCols.Series.INDICATOR_NAME
                                                  )
                                               .trim();
            
            // Using regular expresion to get column values that match key words
            // female and education
            if(indicatorName.matches(secondLevelPattern)){
                //Writing the valuable col vals to context
                context.write(new Text(seriesCode),new Text(":"+indicatorName));
            }
            
        } 
        
    }
    
}