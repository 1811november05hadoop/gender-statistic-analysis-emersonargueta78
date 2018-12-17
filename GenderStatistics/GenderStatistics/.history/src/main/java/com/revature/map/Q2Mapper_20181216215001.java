package com.revature.map;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import com.revature.util.Cleaning;
import com.revature.util.GenderStatsCols;
import com.revature.util.Globals;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Q1Mapper
 */
public class Q2Mapper extends Mapper<LongWritable,Text,MapWritable,MapWritable> implements Globals{
    private static String[] yearlyRanges;
    private static String[] regexes;
    private Cleaning cleaning = new Cleaning();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
        
        String line = value.toString();
        
        Entry<StringBuilder,ArrayList<String>> entry = cleaning.splitAndClean(line);
        StringBuilder stringRow = entry.getKey();
        List<String> colVals = entry.getValue();

        
        StringBuilder dataVals = cleaning
        .getYearlyColumnVals(colVals, new AbstractMap.SimpleImmutableEntry<>(yearlyRanges[0], yearlyRanges[1]));
        String indicatorName = colVals.get(GenderStatsCols.Data.INDICATOR_NAME);
        String country = colVals.get(GenderStatsCols.Data.COUNTRY);

        
        
        if(
            indicatorName.matches(regexes[0])
            && indicatorName.matches(regexes[1])
        ){
            MapWritable mapKey = new MapWritable();          
            MapWritable mapVal = new MapWritable();
            String bq1DataVals = averageIncrease(colVals);

            mapKey.put(new Text(indicatorName), new Text(country));
            mapVal.put(new Text(bq1DataVals),new IntWritable(1));
            context.write(mapKey,mapVal);
            
            
        }
        
    }

    private String averageIncrease(List<String> colVals) {
        StringBuilder retStr = new StringBuilder();
        List<String> dataValsList = cleaning.splitDelimiter(dataVals,",");
        
        for (int year = Integer.valueOf(yearlyRanges[0]); year < Integer.valueOf(yearlyRanges[1]); year++) {
            //Skipping empty column data values
            if(
                colVals.get( dataColsYear.get(String.valueOf(year)) ).length() != 0
            ){
                double yearlyData = Double.valueOf(colVals.get(dataColsYear.get(String.valueOf(year))));
                
                dataValuesMap.put(year,yearlyData);
                strData.append("|"+year+":"+yearlyData);
            }
        }

        for (int index = 0; (index+1) < dataValsList.size(); index++) {
            String [] yearAndData = dataValsList.get(index).split(":",-1);
            String year = yearAndData[0];
            String data = yearAndData[1];
            String [] nextYearAndData = dataValsList.get(index+1).split(":",-1);
            String nextYear = nextYearAndData[0];
            String nextData = nextYearAndData[1];

            if(!data.isEmpty() && !nextData.isEmpty()){
                retStr.append(year+"-"+nextYear+":")
                      .append(caclulatePercentIncrease())
                      .append(",");
            }    
        }
        return retStr.toString();
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

    @Override
    public void setRegexes(String... regexes) { 
        this.regexes = regexes;
    }


    @Override
    public String[] getRegexes() {
        return this.regexes;
    }

    @Override
    public void setYearlyRanges(String... ranges) {
        this.yearlyRanges = ranges;
    }

    @Override
    public String[] getYearlyRanges() {
        return this.yearlyRanges;
    }
    
}