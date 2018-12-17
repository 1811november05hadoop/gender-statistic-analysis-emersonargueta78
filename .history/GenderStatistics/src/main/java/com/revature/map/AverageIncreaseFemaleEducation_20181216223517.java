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
import com.revature.Util.Regex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * ColumnValueMapper
 */
public class AverageIncreaseFemaleEducation extends Mapper<LongWritable, Text, Text, Text> {
    final private static String secondLevelPattern = new Regex().getRegexes()[1];
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        String line = value.toString();

        Entry<StringBuilder,ArrayList<String>> entry = new Cleaning().splitAndClean(line);
        
        ArrayList<String> colValsList = entry.getValue();
        StringBuilder stringRow = entry.getKey();

        int[] yearRange = {FemaleEducation.lowerYearRange,FemaleEducation.upperYearRange};
        
        if(
           stringRow.toString().matches(secondLevelPattern)
          ){
            getCountryAndYearData(colValsList,
            context,
            yearRange
           );
        }
    }

    public static void main(String[] args) {
        
    }

    private void getCountryAndYearData(List<String> colValsList,
                                       Context context,
                                       int[] rangeOfYears
                                      )
     throws IOException, InterruptedException {
        
        String indicatorCode = colValsList.get(GenderStatsCols.Data.INDICATOR_CODE).trim();
        String country = colValsList.get(GenderStatsCols.Data.COUNTRY).trim();
        StringBuilder strData = new StringBuilder();

        //Checking the last range of years of values
        //Getting the last range of years of values 
        Map<String,Integer> dataColsYear = new GenderStatsCols().getDataCols();
        Map<Integer,Double> dataValuesMap = new TreeMap<>();
        
        for (int year = rangeOfYears[0]; year < rangeOfYears[1]; year++) {
            //Skipping empty column data values
            if(
                colValsList.get( dataColsYear.get(String.valueOf(year)) ).length() != 0
            ){
                double yearlyData = Double.valueOf(colValsList.get(dataColsYear.get(String.valueOf(year))));
                
                dataValuesMap.put(year,yearlyData);
                strData.append("|"+year+":"+yearlyData);
            }
        }

        if(dataValuesMap.size() > 0){    
            context.write(
                new Text(indicatorCode),
                new Text(country+":"+caclulatePercentIncrease(dataValuesMap))
            );
            
        }
        
    }

    private String caclulatePercentIncrease(Map<Integer,Double> dataValues) {
        double averageVal = 0.0;
        double ret = 0.0;
        int count = 0;
        
        for (int key : dataValues.keySet()) {
            if(dataValues.containsKey(key+1)){
               averageVal = dataValues.get(key+1) - dataValues.get(key);
               averageVal /= ((key+1) - key);
               ret += averageVal;
               count++;
            }
        }
        if(count >0){
            ret /= ((double)count);
        }
        else{
            ret = 0.0;
        }
        
        return String.valueOf(ret);
    }
}