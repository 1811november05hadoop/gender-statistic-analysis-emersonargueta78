package com.revature.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.revature.Drivers.FemaleGraduates;
import com.revature.Util.Cleaning;
import com.revature.Util.GenderStatsCols;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * ColumnValueMapper
 */
public class ColumnValueMapper extends Mapper<LongWritable, Text, Text, Text>{
    //Used to extract valuable columns
    


    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();

        Entry<StringBuilder,ArrayList<String>> entry = new Cleaning().splitAndClean(line);
        
        ArrayList<String> colValsList = entry.getValue();
        

        int[] yearRange = {FemaleGraduates.lowerYearRange,FemaleGraduates.upperYearRange};
        
        getCountryAndYearData(colValsList,
                              context,
                              yearRange
                             );
        
    }

    private void getCountryAndYearData(List<String> colValsList,
                                       Context context,
                                       int[] rangeOfYears
                                      )
     throws IOException, InterruptedException {
        
        StringBuilder theData = new StringBuilder();

        String indicatorCode = colValsList.get(GenderStatsCols.Data.INDICATOR_CODE).trim();
        String country = colValsList.get(GenderStatsCols.Data.COUNTRY).trim();

        
        //Getting the indexes for columns with yearly data
        Map<String,Integer> dataColsYear = new GenderStatsCols().getDataCols();

        //Getting data from a range of years
        for (int i = rangeOfYears[0]; i < rangeOfYears[1]; i++) {
            if(
                colValsList.get( dataColsYear.get(String.valueOf(i)) ).length() != 0 &&
                Double.valueOf(colValsList.get(dataColsYear.get(String.valueOf(i))))<30.0
            ){
                theData.append("|year:"+i+"::")
                .append( colValsList.get(dataColsYear.get(String.valueOf(i))) );
            }
        }

        //Writing the valuable col vals to context
        if(theData.toString().length() != 0){
            context.write(
                new Text(indicatorCode),
                new Text(country+":"+theData.toString())
               );
        }
        
    }   
}