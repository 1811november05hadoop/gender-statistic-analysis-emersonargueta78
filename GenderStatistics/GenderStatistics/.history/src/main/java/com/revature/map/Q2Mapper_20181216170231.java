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
public class Q2Mapper extends Mapper<LongWritable, Text, MapWritable, MapWritable> implements Globals {
    private static String[] yearlyRanges;
    private static String[] patternToMatch;
    private Cleaning cleaning = new Cleaning();
    
    public static void main(String[] args) throws IOException {
        String testStr = "\"United States\",\"USA\",\"Educational attainment, at least completed lower secondary, population 25+, female (%) (cumulative)\",\"SE.SEC.CUAT.LO.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"22.39346\",\"23.86751\",\"22.2097\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",";
        Entry<StringBuilder,ArrayList<String>> entry = new Cleaning().splitAndClean(testStr);
        List<String> colVals = entry.getValue();

        StringBuilder dataVals = new Cleaning()
        .getYearlyColumnVals(colVals, new AbstractMap.SimpleImmutableEntry<>("2000", "2016"));
        String bq1DataVals = new Q2Mapper().averageIncrease(dataVals.toString());
        System.out.println(bq1DataVals);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException { 
        String line = value.toString();
        
        Entry<StringBuilder,ArrayList<String>> entry = cleaning.splitAndClean(line);
        List<String> colVals = entry.getValue();

        StringBuilder dataVals = cleaning
        .getYearlyColumnVals(colVals, new AbstractMap.SimpleImmutableEntry<>(yearlyRanges[0], yearlyRanges[1]));
       
        String indicatorName = colVals.get(GenderStatsCols.Data.INDICATOR_NAME);
        String country = colVals.get(GenderStatsCols.Data.COUNTRY);
        
        if(
            indicatorName.matches(patternToMatch[0])
            && indicatorName.matches(patternToMatch[1])
        ){
            MapWritable mapKey = new MapWritable();          
            MapWritable mapVal = new MapWritable();
            String bq1DataVals = averageIncrease(dataVals.toString());

            mapKey.put(new Text(indicatorName), new Text(country));
            mapVal.put(new Text(bq1DataVals),new IntWritable(1));
            context.write(mapKey,mapVal);   
        }   
    }

    private String averageIncrease(String dataVals) {
        StringBuilder retStr = new StringBuilder();
        List<String> dataValsList = cleaning.splitDelimiter(dataVals,",");
        
        for (int index = 0; (index+1) < dataValsList.size(); index++) {
            String [] yearAndData = dataValsList.get(index).split(":",-1);
            String year = yearAndData[0];
            String data = yearAndData[1];
            String [] nextYearAndData = dataValsList.get(index+1).split(":",-1);
            String nextYear = nextYearAndData[0];
            String nextData = nextYearAndData[1];

            if(!data.isEmpty() && !nextData.isEmpty()){
                retStr.append(year+"-"+nextYear+":")
                      .append(averageIncreaseHelper(data,nextData))
                      .append(",");
            }    
        }
        return retStr.toString();
    }

    private String averageIncreaseHelper(String data, String nextData) {
        double dataD = Double.valueOf(data);
        double dataDNext = Double.valueOf(nextData);

        return String.valueOf(dataDNext - dataD);
    }

    @Override
    public void setRegexes(String... patternToMatch) { 
        this.patternToMatch = patternToMatch;
    }


    @Override
    public String[] getRegexes() {
        return this.patternToMatch;
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