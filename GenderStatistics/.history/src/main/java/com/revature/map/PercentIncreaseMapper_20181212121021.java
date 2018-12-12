package com.revature.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
public class PercentIncreaseMapper extends Mapper<LongWritable, Text, Text, Text> {

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
        for (int i = rangeOfYears[0]; i < rangeOfYears[1]; i++) {
            if(
                colValsList.get( dataColsYear.get(String.valueOf(i)) ).length() != 0
            ){
                
                dataValues.add(Double.valueOf(colValsList.get(dataColsYear.get(String.valueOf(i)))));
            }
        }
        if(dataValues.size() > 1){    
            //Writing the valuable col vals to context
            context.write(
                new Text(indicatorCode),
                new Text(country+":"+caclulatePercentIncrease(dataValues))
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
        ret /= ((double)numVals-1.0);
        
        return String.valueOf(ret);
    }
}