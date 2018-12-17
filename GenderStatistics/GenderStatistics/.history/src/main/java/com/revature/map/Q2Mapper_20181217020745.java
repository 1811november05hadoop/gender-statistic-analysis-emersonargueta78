package com.revature.map;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.revature.util.Cleaning;
import com.revature.util.GenderStatsCols;
import com.revature.util.Globals;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Q1Mapper
 */
public class Q2Mapper extends Mapper<LongWritable,Text,Text,Text> implements Globals{
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
            indicatorName.contains(regexes[3])
            && indicatorName.contains(regexes[2])
            && indicatorName.contains(regexes[1])
            && indicatorName.contains(regexes[0])
        ){
            
            String bq1DataVals = averageIncrease(colVals);

            context.write(new Text(indicatorName+"|"+country),new Text(bq1DataVals));
            
            
        }
        
    }

    // public static void main(String[] args) throws IOException {
    //     String line = "\"United States\",\"USA\",\"Educational attainment, completed Bachelor\'s or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"5\",\"3\",\"2\","; 
    //     Entry<StringBuilder,ArrayList<String>> entry = new Cleaning().splitAndClean(line);
    //     List<String> colVals = entry.getValue();
    //     Q2Mapper q2Mapper = new Q2Mapper();

    //     q2Mapper.setYearlyRanges("2000","2016");
    //     System.out.println(q2Mapper.averageIncrease(colVals).equals("0.125"));

    // }
    
    private String averageIncrease(List<String> colVals) throws IOException {        
        Map<String,Integer> dataColsYear = new GenderStatsCols().getDataCols();
        Map<Integer,Double> dataValuesMap = new HashMap<>();

        for (int year = Integer.valueOf(yearlyRanges[0]); year <= Integer.valueOf(yearlyRanges[1]); year++) {
            //Skipping empty column data values
            if(
                colVals.get( dataColsYear.get(String.valueOf(year)) ).length() != 0
            ){
                double yearlyData = Double.valueOf(colVals.get(dataColsYear.get(String.valueOf(year))));
                dataValuesMap.put(year,yearlyData);
            }
            else{
                dataValuesMap.put(year,0.0);
            }
        }
        if(dataValuesMap.size() > 0){
            return caclulatePercentIncrease(dataValuesMap);
        }
        return "";
    }

    private String caclulatePercentIncrease(Map<Integer,Double> dataValues) {
        double averageVal = 0.0;
        double ret = 0.0;
        int count = 0;
        
        for (int year : dataValues.keySet()) {
            if(dataValues.containsKey(year+1)){
               averageVal = dataValues.get(year+1) - dataValues.get(year);
               averageVal /= ((year+1) - year);
               //System.out.println("hello ret val: " +ret);
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