package com.revature.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import com.revature.GenderStatistics;
import com.revature.Drivers.FemaleEducation;
import com.revature.Util.Cleaning;
import com.revature.Util.GenderStatsCols;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * ColumnValueMapper
 */
public class ColumnValueMapper extends Mapper<LongWritable, Text, Text, Text>{
    //Used to extract rows with words education and female
    // final private static String searchEducationFemaleCodes = "(.*)Educational(.*).*(.*)completed(.*).*(.*)female(.*)";
    // final private static String searchEducationCodes = "(.*)education(.*)";


    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();

        Entry<StringBuilder,ArrayList<String>> entry = new Cleaning().splitAndClean(line);
        
        ArrayList<String> colValsList = entry.getValue();
        
        int[] yearRange = {FemaleEducation.lowerYearRange,FemaleEducation.upperYearRange};

        getCountryAndYearData(colValsList,context,yearRange);
        
    }

    private void getCountryAndYearData(List<String> colValsList,
                                       Context context,
                                       int[] rangeOfYears
                                      )
     throws IOException, InterruptedException {
        
        //Getting the country
        StringBuilder graduateData = new StringBuilder();

        String indicatorCode = colValsList.get(GenderStatsCols.Data.INDICATOR_CODE).trim();
        String country = colValsList.get(GenderStatsCols.Data.COUNTRY).trim();

        //Checking the last range of years of values
        //Getting the last range of years of values 
        Map<String,Integer> dataColsYear = new GenderStatsCols().getDataCols();
        for (int i = GenderStatsCols.Data.YEAR.length-1,j=0;j<rangeOfYears; i--,j++) {
            if(
                colValsList.get(GenderStatsCols.Data.YEAR[i]).length() != 0 &&
                Double.valueOf(colValsList.get(GenderStatsCols.Data.YEAR[i]))<30.0
            ){
                graduateData.append("|year:"+(GenderStatsCols.Data.LAST_YEAR - j)+"::")
                .append(colValsList.get(GenderStatsCols.Data.YEAR[i]));
            }
            
        }

        //Writing the valuable col vals to context
        if(graduateData.toString().length() != 0){
            context.write(
                new Text(indicatorCode),
                new Text(country+":"+graduateData.toString())
               );
        }
        
    }
    
}