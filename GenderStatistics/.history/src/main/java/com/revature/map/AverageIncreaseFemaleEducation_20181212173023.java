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
public class AverageIncreaseFemaleEducation extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();

        Entry<StringBuilder,ArrayList<String>> entry = new Cleaning().splitAndClean(line);
        
        ArrayList<String> colValsList = entry.getValue();
        

        int[] yearRange = {FemaleEducation.lowerYearRange,FemaleEducation.upperYearRange};
        
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
        
        

        String indicatorCode = colValsList.get(GenderStatsCols.Data.INDICATOR_CODE).trim();
        String country = colValsList.get(GenderStatsCols.Data.COUNTRY).trim();

        //Checking the last range of years of values
        //Getting the last range of years of values 
        Map<String,Integer> dataColsYear = new GenderStatsCols().getDataCols();
        ArrayList<Double> dataValues = new ArrayList<>();
        Map<Integer,Double> dataValuesMap = new TreeMap<>();
        for (int year = rangeOfYears[0]; year < rangeOfYears[1]; year++) {
            //Skipping empty column data values
            if(
                colValsList.get( dataColsYear.get(String.valueOf(year)) ).length() != 0
            ){
                
                //dataValues.add(Double.valueOf(colValsList.get(dataColsYear.get(String.valueOf(i)))));
                dataValuesMap.put(year, Double.valueOf(
                                                        colValsList.get(
                                                                        dataColsYear.get(String.valueOf(year))
                                                                        )
                                                      )
                                 );
            }
        }
        if(dataValuesMap.size() > 0){    
            //Writing the valuable col vals to context
            context.write(
                new Text(indicatorCode),
                new Text(country+":"+caclulatePercentIncrease(dataValuesMap))
            );
        }
        
    }

    private String caclulatePercentIncrease(ArrayList<Double> dataValues) {
        int numVals = dataValues.size();
        double ret = 0.0;

        for (int i = 0; i < numVals;i++) {
            if(i+1 < numVals)
            ret += dataValues.get(i+1) - dataValues.get(i);
        }
        //ret /= ((double)numVals-1.0);
        
        return String.valueOf(ret);
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
               System.out.println("current avaerage: "+ret);
               count++;
            }
        }
        ret /= ((double)count);

        return String.valueOf(ret);
    }
}